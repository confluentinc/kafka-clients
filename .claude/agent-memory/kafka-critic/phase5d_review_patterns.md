---
name: Phase 5d NetworkClient review patterns
description: NetworkClient + NetworkClientUtils translation gotchas — multi-arm error fanout in doSend, KIP-511 untested branches, test-name overclaim
type: project
---

# Phase 5d — NetworkClient + NetworkClientUtils review patterns

## High-yield review axes

1. **`doSend` UnsupportedVersion fan-out has THREE arms**: `!internal`,
   `internal+METADATA`, `internal+TELEMETRY`. The Rust translation tends
   to only land the first (push to `aborted_sends`) and silently drop
   the second. Phase 5d uses `ManualMetadataUpdater` so the bug is
   latent — but it WILL manifest in Phase 6 with `DefaultMetadataUpdater`.
   Always cross-check `do_send`'s error branch against ALL of Java's
   else-if arms in `NetworkClient.java:583-598`.

2. **Internal-vs-user response routing in `handleCompletedReceives`**
   has FOUR cases: internal+METADATA, internal+API_VERSIONS,
   internal+telemetry (×2), and "everything else". Telemetry is
   skipped per Phase 5d scope; verify the Rust code doesn't
   accidentally push internal MetadataResponses to user-visible
   responses. Hardcoded api-id literals (`api_id == 3`,
   `api_key == 18`) are a code smell — flag if the actor uses raw
   integers instead of named ApiKeys constants but accept it as a
   Suggestion, not a Bug.

3. **`handleApiVersionsResponse` KIP-511 fallback path**: the non-
   trivial else branch extracts `max_version` from the response's
   `api_keys.find(API_VERSIONS.id)` and re-queues a downgraded
   request. Java has dedicated tests
   (`testUnsupportedApiVersionsRequestWithVersionProvidedByTheBroker`
   and the "without" companion). If the Rust port has only a
   "happy-path API_VERSIONS handshake" test and an "invalid response
   closes connection" test, the KIP-511 fallback re-queue is
   UNTESTED — flag it.

## Test-name semantic drift

The actor's `disconnect_marks_node_failed_AND_RESPECTS_BACKOFF` test
asserts only `is_ready=false` + `connection_failed=true`. The Java
parent test additionally exercises:
- `canConnect=false` immediately after disconnect (backoff active)
- `time.sleep(reconnectBackoffMaxMs); canConnect=true` (backoff expired)
- `disconnect()` again; `canConnect=true` still (re-disconnect doesn't
  reset backoff window)

Test names that include behavior claims like "respects_backoff",
"validates_X", "fan_outs_to_Y" — verify the assertions back the
claim. A test whose body doesn't exercise the named property is a
documentation bug.

## Untested 60-line code paths (Phase 5d examples)

- `least_loaded_node` (60 LOC, 4-tier preference order with 4
  dedicated Java tests) — translated but ZERO tests in Rust.
- `send_and_receive` (Java has 4 distinct error exit paths) —
  translated but ZERO tests for any error arm.
- `cancel_in_flight_requests` for multi-in-flight disconnect —
  translated, single-request test exists, multi-request fan-out
  test absent.

Pattern: when a Java method has dedicated `testFooBranch1`,
`testFooBranch2`, `testFooBranch3` tests, and the Rust port has
ONE happy-path test, the branches with no Rust coverage are real
gaps regardless of how many Java tests SAY they're covered.

## Defensible Phase 5d deferrals — verify and accept

- **TLS handshake at NetworkClient layer**: if Phase 5b-2 already
  drives a real rcgen self-signed handshake AND Phase 5b-3 wires
  it into `KafkaChannel` AND Phase 5c-2 exercises through Selector,
  then re-running through NetworkClient adds plumbing-only coverage.
  PLAN.md "TLS handshake test connects to a self-signed broker"
  can read literally OR end-to-end; literal reading is fine if
  lower layers are pinned.
- **Skipped Java tests**: testReconnectAfterAddressChange (Mockito-
  driven), testRebootstrap (needs DefaultMetadataUpdater), telemetry
  tests, throttling tests, connection-setup-timeout tests covered
  by Phase 4c — all defensible.

## Hot-path audit checklist for `do_send`

Per CLAUDE.md rule 11/12, verify on every send:
1. No `String` clone of node id — `Arc<str>` reused via `node_labels`
   cache populated at `initiate_connect`.
2. No intermediate `Vec<u8>` copies between `serialize_with_header`
   and the wire — Java does one body-bytes Vec, the Rust port
   should match (acceptable: 1× body Vec + 1× 4-byte size prefix
   `Bytes` + 1× `Box<dyn Send>`).
3. `client_request.destination_arc()` should clone the existing
   Arc, NOT allocate a fresh `Arc::from(format!(...))`.

## Constants placement

`MAX_RESERVED_CORRELATION_ID` / `MIN_RESERVED_CORRELATION_ID` belong
to `SaslClientAuthenticator` in Java; Phase 9 will eventually
translate that. Mirror the constants in `network_client.rs` for now
(the only consumer); Phase 9 should re-export from the eventual
`SaslClientAuthenticator` module. Acceptable as a Phase 5d-internal
solution.

## Wrapping<i32> for Java post-increment overflow

Java's `int correlation; ... return correlation++;` wraps to negative
on overflow ("the numeric overflow is fine as negative values is
acceptable" per Java comment). Rust's `Wrapping(i32)` mirrors this
without `unsafe`. Verify:
- `is_reserved_correlation_id(i32::MIN)` returns false (since
  `i32::MIN < MIN_RESERVED_CORRELATION_ID == i32::MAX - 7`).
- `Wrapping(MAX_RESERVED) + Wrapping(1) == Wrapping(i32::MIN)`.
- Post-increment `correlation += Wrapping(1)` continues from
  `i32::MIN` to `i32::MIN + 1` correctly.

## `&mut self` on trait method with no Java mutability marker

Java's `KafkaClient.newClientRequest` is unmarked but mutates
`this.correlation`. Class doc says "not thread-safe". The Rust trait
adopts `&mut self` to make the contract explicit — accept this as
defensible, NOT a divergence to flag. Phase 6 producer must hold
client behind exclusive access (single-task-per-client pattern).
