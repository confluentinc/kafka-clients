---
name: Phase 5d review fixes
description: Patterns from Phase 5d review round — MockTime jitter math, MetadataUpdater test mocks, KIP-511 same-poll re-dispatch
type: project
---

Patterns and traps from the Phase 5d Round 1 review fixup round:

## MockTime fixture: backoff sleep interval depends on max_interval, not initial

**Why:** First disconnect's `reconnect_backoff_ms = backoff(failed_attempts=0) = initial_interval ± 20% jitter`. With `create_client` fixture's `(initial=10_000, max=100_000)`, that's `~10_000 ms` for the first disconnect. Sleeping `5_000 ms` (the `reconnect_backoff_max` from a *different* fixture) is too short. Always use the constructor argument that matches the fixture, not the Java reference fixture's value.

**How to apply:** When porting Java tests that `time.sleep(reconnectBackoffMaxMsTest)`, look up the actual `reconnect_backoff_max` value passed to `NetworkClient::new` in the Rust fixture — it may differ from the Java fixture. The `create_client` helper in `network_client.rs::tests` uses `(10_000, 100_000)`.

## MetadataUpdater test mock pattern

**Why:** `ManualMetadataUpdater::handle_failed_request` is a no-op (correct for production), but tests need to assert it WAS called. Wrap a `ManualMetadataUpdater` in a `RecordingMetadataUpdater` that delegates everything except `handle_failed_request`, which appends to an `Arc<Mutex<Vec<Option<KafkaError>>>>`.

**How to apply:** When testing `cancel_in_flight_requests` / `do_send` paths that fire `handle_failed_request`, use a recording wrapper. Don't try to monkey-patch `ManualMetadataUpdater`.

## KIP-511 fallback re-dispatches in the SAME poll

**Why:** `NetworkClient::poll` runs `handle_completed_receives` (which sets `nodes_needing_api_versions_fetch[node] = with_version(broker_max)`) BEFORE `handle_initiate_api_version_requests` (which drains that map and sends the v2 request). So a single poll consumes the response AND dispatches the fallback — `nodes_needing_api_versions_fetch` will be empty after the poll completes, not populated.

**How to apply:** Don't assert `nodes_needing_api_versions_fetch.contains_key(&node.id())` between the two passes — that intermediate state isn't observable from outside `poll`. Instead, assert via `InFlightRequests::last_sent(node.id()).header.api_version()` that the new in-flight is at the expected fallback version. Same applies to any other "queue → drain" pair within a single `poll`.

## Hostname-resolution gotcha in multi-node tests

**Why:** `NetworkClient::initiate_connect` calls `connection_states.current_address` which uses the real `DefaultHostResolver`. Hostnames like `"a"` / `"b"` will fail DNS lookup, leaving the node in disconnected state — `is_ready` never returns true.

**How to apply:** In multi-node tests use distinct ports on `"localhost"` (e.g. `Node::new(0, "localhost", 9092)` + `Node::new(1, "localhost", 9093)`) — the i32 ids keep the nodes distinct end-to-end and DNS resolution succeeds in the unit-test environment.

## `Arc::clone(&time as Arc<MockTime>)` requires explicit cast for `Arc<dyn Time>`

**Why:** `NetworkClient::new` takes `Arc<dyn Time>`. `Arc::clone(&Arc<MockTime>)` produces `Arc<MockTime>` — Rust can't auto-coerce on `Arc::clone` because the function signature is `fn clone(&self) -> Self`. The widening happens when assigning to a typed slot.

**How to apply:** Use `Arc::clone(&time) as Arc<dyn crate::common::utils::Time>` explicitly when handing to `NetworkClient::new`. The `create_client` test helper handles this internally; ad-hoc constructions outside it must spell the coercion.
