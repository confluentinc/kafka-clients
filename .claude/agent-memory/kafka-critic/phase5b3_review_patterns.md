---
name: Phase-5b-3 review patterns
description: KafkaChannel + ChannelBuilders translation review — eager-vs-lazy principal cache, test fidelity gaps for partial-write and ConfigDef-driven filters
type: project
---

Patterns surfaced in Phase 5b-3 review (commit `253c383`, `KafkaChannel` +
`ChannelBuilder` + `ChannelBuilders` + Plaintext/SslChannelBuilder + minimal
Authenticator trait).

## Eager-vs-lazy principal/state cache trap

**Pattern**: Java often computes a value lazily on each method call (e.g.
`SslAuthenticator.principal()` reads from the live transport's peer cert
chain on every call). Rust translations sometimes "optimize" by caching at
construction time. This silently breaks correctness when the underlying
state changes after construction.

**Concrete instance**: `SslChannelBuilder::build_ssl_channel` constructs the
`SslTransportLayer` with a fresh, pre-handshake rustls `ClientConnection`,
then immediately calls `SslAuthenticator::new(&transport)` which calls
`peer_principal()` and caches the result. Pre-handshake there are no peer
certs → `KafkaPrincipal::anonymous()` is cached forever. Post-handshake,
calls to `KafkaChannel::principal()` continue to return anonymous instead
of the actual cert DN.

**How to spot**: When a Rust struct field is set from a method call that
in Java is on a different object's interface (and Java calls the method
each time), check whether the source state changes between construction
and use. If yes, the cache is wrong.

**Defensible-cache test**: trace from construction site → first use site
through any state-changing method on the source. If `Self::source.X()`
varies post-construction (e.g. `peer_certificates()` populated by a TLS
handshake), the cache is broken.

## Test fidelity vs Java assertions

**Pattern**: Rust test claims to translate a Java test, but silently
elides assertions OR replaces them with the inverse fact. The test still
passes — and looks like coverage — but doesn't actually verify the same
behavior.

**Concrete instances** in `channel_builder_configs_listener_prefix`:
1. Java's `assertNull(configs.get("plain.sasl.server.callback.handler.class"))`
   — Java's typed-config-driven filter drops it. Rust impl keeps it. Rust
   test omits the assertion (replaced with a confused "wait, let me re-read"
   comment block).
2. Java's `assertEquals("custom.config1", configs.get("listener.name.listener1.gssapi.config1.key"))`
   — Java's `originals()` filter retains the prefixed form. Rust impl
   strips the prefix. Rust test asserts the stripped form
   `gssapi.config1.key` instead of the original prefixed form.

**How to spot**: Read the Java test top-to-bottom and grep the Rust test
file for each Java assertion's literal key/value. Missing assertions or
assertions with a different key are red flags.

**Multi-tick mock semantics**: Java's KafkaChannelTest.testSending
configures Mockito to return 4L, 64L, 64L on three consecutive `write()`
calls — exercising the `bytes_remaining > 0 → maybe_complete_send() == None`
contract across multiple Selector ticks. Rust's `MockTransport` collapses
to single-tick "writes everything". The test misses the partial-progress
contract that prevents the Selector from advancing too eagerly. To
mirror correctly, the mock needs an optional `max_bytes_per_write` cap.

## Java package-private → Rust visibility

**Pattern**: Java's package-private methods (no visibility keyword) are
callable from anywhere in `org.apache.kafka.common.network`. The closest
Rust equivalent for the same trust boundary is `pub(crate)` — broader
than Java's package-private but narrower than `pub`. CLAUDE.md rule 2
mandates `pub(crate)` only for `internal/...` packages, but a faithful
translation of package-private should still use `pub(crate)`.

**Concrete instance**: `KafkaChannel::mute`, `maybe_unmute`, and
`complete_close_on_authentication_failure` are Java-package-private
(callable from `Selector.java` in the same package). Phase 5b-3 made
them `pub`, exposing them on the public crate API. The Phase 5c
Selector (sibling module in the same crate) works fine with
`pub(crate)`.

**Verification**: `grep -rn '\.mute()\|\.maybe_unmute()' src/` returns
only test usage. Phase 5c is the next consumer. `pub(crate)` is the
correct visibility.

## Eager string-allocation in Result error path

**Pattern**: When the Result error path computes a string description
(e.g. `remote_desc = self.remote_address.as_ref().map(|a| a.to_string())`),
the allocation happens before branching on whether the description is
actually used. If only one branch consumes it, the other branch wastes
the allocation. Tiny — usually not worth flagging unless on a hot path
or in a tight retry loop.

**How to apply**: Skip flagging unless the function is called per-message
or per-tick. `prepare()` runs once per channel lifecycle, so this is
not worth a comment.

## Java InetAddress vs Rust SocketAddr

**Pattern**: Java's `InetAddress` is host-only (no port). Rust's
`SocketAddr` is host + port. When translating Java `socketAddress():
InetAddress`, returning a `SocketAddr` is wider than the Java surface
and conflates with the separate `socketPort()` method.

**How to apply**: For methods that take their semantics from the Java
return type, preserve the type fidelity (e.g. return `IpAddr` for
`InetAddress`, separate accessor for port). Otherwise document the
divergence and split into two methods. Worth a Suggestion-severity
comment.

## What to skip flagging

- Eagerly-computed unused string in error path of one-shot lifecycle
  method (`prepare()`).
- `+ Send` redundancy on `Box<dyn Trait>` when the trait already
  declares `: Send` — cosmetic.
- Single-tick test simplification when the actor's docstring
  acknowledges the Java multi-tick mock and the lifecycle contract is
  exercised. Suggestion at most, not blocking.

## Round-2 finding: asymmetric host-vs-host:port in fallback branch

**Pattern**: When translating Java's `socketDescription()` (and similar
multi-branch log-string builders), every Java branch consults
`InetAddress.toString()` (host only). The Rust translation may use
`addr.ip().to_string()` for the easy branches but `local.to_string()`
where `local: SocketAddr` for the fallback — emitting `host:port`
where Java emits host only. The branches must use the same level
(IP) for symmetry and Java fidelity. Found in Phase 5b-3 fixup
`281a4a9` `KafkaChannel::socket_description`.

**How to spot**: Read every fallback branch and check the type at
the `.to_string()` call. `IpAddr::to_string()` ≠ `SocketAddr::to_string()`
even when the SocketAddr came from the same source IP — the latter
appends `:port`.

## Verified-safe patterns

- **`is_in_mutable_state` Java→Rust**: Java's `if (receive == null ||
  receive.memoryAllocated()) return false; return transportLayer.ready();`
  translates cleanly to `match self.receive { None => false, Some(r) if
  r.memory_allocated() => false, _ => self.transport_layer.ready() }`.
  Match-arm-with-guard preserves both early-return semantics.
- **State-machine `if`-after-`if` order**: Java's `RESPONSE_SENT` /
  `THROTTLE_ENDED` cases use two consecutive `if`s where the first may
  reassign the state and the second tests the new value. The Rust
  translation must use two `if`s, NOT `if`/`else if`, to preserve the
  fall-through semantic (in Java, after the first `if` reassigns to
  `MUTED`, the second `if` checks `MUTED_AND_THROTTLED_AND_RESPONSE_PENDING`
  — false now, so doesn't fire). Direct translation is correct.
- **`Arc<str>` for connection id**: hot-path identifier, Java uses
  `String`; Rust uses `Arc<str>` with `id_arc()` accessor for cheap
  cloning into `NetworkSend`. Per CLAUDE.md rule 11.
