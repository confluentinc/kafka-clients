---
name: Phase 8a Critic-8-archive premise error + visibility leakage
description: integration-test `tests/<name>/main.rs` is a SEPARATE crate; pub(crate) types in public constructor return positions block downstream callers
type: project
---

Phase 8a surfaced two facts that are likely to recur in future
Milestones whenever a public producer/consumer constructor returns
a generic type bound by a pub(crate) inner type.

## Fact 1: Integration tests are downstream, not same-crate

Critic 8's Phase 8.0 Suggestion 2 archive (`COMMENTS.DONE.8.md`)
claimed:

> Demoted [`from_config`] to `pub(crate)` ... `tests/integration/
> performance_test.rs` continues to compile because it lives in
> the same crate.

This is **wrong**. Each Cargo integration test in `tests/<name>/main.rs`
(or `tests/<name>.rs`) is a separate crate that links against the
library as a *downstream consumer*. It can only see `pub` items.
`performance_test.rs` compiled only because it was `mod`-commented-
out in `tests/integration/main.rs` — never actually built.

**Why:** Apply when reviewing visibility decisions on Kafka public-
API methods. If the method's return type names any non-`pub` item,
the public constructor is unusable from `tests/integration/*` —
which Phase 8 onward depends on.

**How to apply:** Before demoting any type or trait that appears
transitively in a `pub fn` return type to `pub(crate)`, build
`cargo build --features integration-tests --tests` with the
intended integration test wired in (not commented out). If that
build fails with `error: type ... is private`, the demotion is
incompatible with the public API.

## Fact 2: `#[doc(hidden)]` is the right cordon

When Rust's visibility model forces a `pub` declaration on a type
that has no Java public-API analogue (like `DefaultMetadataUpdater`,
which is Java's package-private inner class), the right resolution
is `pub` + `#[doc(hidden)]`. This keeps the type:
- Reachable by name in downstream binding sites (so `let producer =
  ctor()` compiles).
- Off the docs.rs surface (preserving the "Java parity only"
  intent — no public API growth).

The cordon should be applied at the **module level** (e.g.
`#[doc(hidden)] pub mod default_metadata_updater;` in lib.rs) so
the whole namespace is excluded, AND at the type level for
defense in depth.

**Why:** Future phases that translate other Java inner classes
(e.g. `Sender.SenderMetrics`, `RecordAccumulator.IncompleteBatches`)
will hit the same visibility wall whenever they appear in a public
constructor's transitive return type. The pattern is reusable.

**How to apply:** When promoting a `pub(crate)` type to `pub` to
satisfy a downstream crate, do all three: (1) `pub` on the module,
(2) `#[doc(hidden)]` on the module, (3) `#[doc(hidden)]` on the
type itself. Update the type's rustdoc to explain the cordon and
to steer callers toward holding the value via a trait (e.g.
`Producer`) or via type inference rather than by name.

## Fact 3: ApiVersions wire-protocol blocker (RUNTIME, not VISIBILITY)

`KafkaProducer::with_serializers` builds a real `NetworkClient<Selector,
DefaultMetadataUpdater>` (Phase 8.0). Against a live Kafka 4.2.0
broker over PLAINTEXT loopback, the producer:
1. TCP-connects (Selector reports connect-complete).
2. Initiates ApiVersions fetch.
3. NEVER gets a response. After `default_request_timeout_ms = 30s`
   the NetworkClient calls `handle_timed_out_requests`,
   disconnects, retries — loops until `max.block.ms = 60s` fires.
4. `wait_on_metadata` returns `Timeout("Topic ... not present in
   metadata after 60000 ms")`.

Topic creation via `docker exec kafka-topics --create + --describe`
succeeds — broker is healthy. The bug is somewhere between Rust's
ApiVersions request serialization and the broker's ability to
parse it (or between the broker's response and the Rust Selector's
ability to decode it). Phase 5d's in-process MockClient tests do
NOT cover the real-wire path.

**Why:** Phase 8a is the FIRST end-to-end Rust-client + real-broker
exchange in the project. Unit tests up to Phase 7f all use
in-process mocks. The wire-protocol byte-level tests in Phase 2d
test request/response codec against known byte vectors, but
those vectors may have drifted from what a Kafka 4.2.0 broker
actually sends/expects.

**How to apply:** Any future "first real wire exchange" milestone
(integration tests, FFI live tests, consumer Fetch loop) should
budget time for a wire-bytes investigation BEFORE assuming the
encoder/decoder is correct. `tcpdump -X -i lo0 port <host_port>`
against a manual broker is the standard diagnostic — compare
against `kafka-console-producer/consumer` running from the same
host to isolate Rust-client vs network issues.
