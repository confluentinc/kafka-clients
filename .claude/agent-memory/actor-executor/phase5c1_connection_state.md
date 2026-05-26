---
name: Phase 5c-1 connection state + traits
description: Phase 5c-1 design choices that downstream Selector/NetworkClient phases must respect
type: project
---

# Phase 5c-1 — what's load-bearing for Phase 5c-2 / 5d

## i32 connection ids end-to-end (NOT String)

`InFlightRequests`, `ClusterConnectionStates`, `Selectable`, and
`KafkaClient` all key by `i32`. Java keys by `String` (always built
from `Integer.toString(node.id())`). Phase 5c-2's Selector and Phase
5d's NetworkClient inherit this — do not introduce a `String` key
anywhere in the wire path. CLAUDE.md rule 11.

The `Arc<str>` "human label" still flows through `ClientResponse` and
`NetworkSend` for diagnostics; that is unrelated to the key plumbing.

## `InFlightRequest` lives in `in_flight_requests.rs` for now

Java's `NetworkClient.InFlightRequest` is a static inner class.
Phase 5c-1 places it next to `InFlightRequests` (its only collection)
so the lock-step ordering invariant (`addFirst`/`pollLast` vs
`pop_back`/`pop_front`) is visible in one file. **Phase 5d may move
it into `network_client.rs` if NetworkClient needs additional
fields**, but the public surface (`InFlightRequest::completed`,
`::timed_out`, `::disconnected`) is already shaped for that move.

## `Selectable` has `async fn poll` + `i32` ids — Java divergences

Two breaks from Java's interface that the Selector (5c-2) must
implement:

1. `async fn poll(&mut self, timeout_ms: i64) -> Result<(), KafkaError>`
   (Java is blocking `void poll(long) throws IOException`).
2. All connection-id parameters are `i32` (Java uses `String`).

Method names diverge slightly to avoid Rust keyword collisions:
- `Selectable.close(String)` → `close_connection(i32)` (Rust's
  `close()` is the no-arg variant).
- `Selectable.muteAll()` → `mute_all()`, `Selectable.unmuteAll()` → `unmute_all()`.

`USE_DEFAULT_BUFFER_SIZE = -1` is a module-level const in
`common::network::selectable` (Java has it as
`Selectable.USE_DEFAULT_BUFFER_SIZE`).

## `ApiVersions::update` accepts `Arc<NodeApiVersions>`

Java passes by reference (Java's everything-is-a-reference). The Rust
translation accepts `Arc<NodeApiVersions>` so the cache can hand out
cheap clones via `get(&self, node_id) -> Option<Arc<NodeApiVersions>>`.
Callers (Phase 5d NetworkClient) wrap a freshly-built
`NodeApiVersions` with `Arc::new(...)` at the call site.

`ApiVersions::get` cannot return `&NodeApiVersions` because the
`Mutex<Inner>` guard would have to live longer than the borrow.
`Arc<...>` is the canonical way to expose a snapshot in this case.

## `SupportedVersionRange` lives in `common::feature::`

New module. Java's `org.apache.kafka.common.feature.SupportedVersionRange`
parent (`BaseVersionRange`) is collapsed onto the same struct since
the only other subclass (`FinalizedVersionRange`) is not used on the
producer path. Re-exported via `common::feature::SupportedVersionRange`.

## `ApiVersionsResponse::intersect` was missing — now added

Java's `static Optional<ApiVersion> intersect(ApiVersion, ApiVersion)`
is the workhorse for `latest_usable_version`. Phase 5c-1 added it as
a static method on `ApiVersionsResponse` (in `common::requests`).
Returns `Result<Option<ApiVersion>, KafkaError>` because the Java
method throws `IllegalArgumentException` on api-key mismatch.

## `MetadataUpdater` trait carries `Option<KafkaError>` for both
disconnect and failed-request

Java passes `Optional<AuthenticationException>` /
`Optional<KafkaException>`. The Rust trait projects both onto
`Option<KafkaError>` — callers should pass
`KafkaError::Authentication` (resp. any `KafkaError`) to mirror the
Java intent. Reduces the trait surface and avoids a separate
`AuthenticationException` shim in this milestone.

## `ClusterConnectionStates` panics on missing-node lookup

`connection_state(id)` and `connection_setup_timeout_ms(id)` panic
with the Java `IllegalStateException` message ("No entry found for
connection {id}") when the entry is missing. Java's contract is
identical (`IllegalStateException`). Per CLAUDE.md rule 10.1 this is
the right shape for an internal API where a missing entry means a
caller violated the implicit contract.

## `connecting()` host-changed log message uses `info!`, not `warn!`

Java logs at INFO level when a node id's hostname changes mid-flight
(rare event but not an error). Same level in Rust to avoid noisy logs
during DNS rebalancing.

## Test fixture `i32` node ids match Java decimal strings

Java uses `"1001"`, `"2002"`, `"3003"` as ids. The Rust translation
uses `1001`, `2002`, `3003` so test failure messages stay grep-able
against Java reference output. The `AddressChangeHostResolver` is
inlined in the test module and uses `Arc<Mutex<...>>` for the
mutable `useNewAddresses`/`resolutionCount` fields (Rust does not
allow `&mut self` from a `&dyn HostResolver`).

## Files left for Phase 5c-2 / 5d

- `Selector` (Tokio implementation of `Selectable`) — Phase 5c-2.
- `NetworkClient` — Phase 5d. Will use `InFlightRequest::completed`,
  `::timed_out`, `::disconnected`; the `#[allow(dead_code)]` on
  those methods drops automatically once 5d wires them.
- The `pub(crate)` flags on `InFlightRequest` / `InFlightRequests`
  are intentional — Java's `static class` (no `public`) is
  package-private, mapped to `pub(crate)` (CLAUDE.md naming rules).
