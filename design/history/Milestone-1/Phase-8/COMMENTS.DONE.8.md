# Critic 8 — Phase 8 resolved comments archive

Each block here was an open item in `COMMENTS.8.md` that Actor 8 has
since fixed. The block must include the fixup commit SHA that resolved
it. Critic 8 verifies each fixup commit before allowing the block to
land here.

---

## Blocking 1: `handleSuccessfulResponse` drops the `metadataRecoveryStrategy == REBOOTSTRAP` gate
- **File**: `src/default_metadata_updater.rs:333-343`
- **Severity**: Blocking
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java:1297`
- **Description**: Java gates the REBOOTSTRAP_REQUIRED branch with **both** `metadataRecoveryStrategy == REBOOTSTRAP` **and** `response.topLevelError() == REBOOTSTRAP_REQUIRED`. The Rust translation only checks the response error, dropping the strategy gate. The inline comment at `default_metadata_updater.rs:336-341` justifies this by claiming "the gate is enforced by the caller (NetworkClient::poll only dispatches to handleRebootstrap when the strategy is REBOOTSTRAP)" — but this conflates two different paths. `NetworkClient::handle_rebootstrap` (line 740) gates the *teardown-and-rebootstrap* step on the strategy, but the **state-mutation side-effects** that Java's gate prevents (the info log, `initiateRebootstrap()` setting `metadataAttemptStartMs = Some(0)`, and the **skipped `metadata.failed_update(now)` call**) all fire regardless of strategy in Rust.

  Concretely, when the broker sends REBOOTSTRAP_REQUIRED and the client's strategy is `None`:
  - Java: hits the `else if (response.brokers().isEmpty())` arm (REBOOTSTRAP_REQUIRED error responses carry an empty broker list), trace-logs, calls `metadata.failedUpdate(now)` so the failed-update backoff advances, clears `inProgress`.
  - Rust: hits the (un-gated) `is_rebootstrap_required` arm, info-logs the wrong message, calls `initiate_rebootstrap()` (mutating `metadata_attempt_start_ms` to `Some(0)` for a strategy that won't act on it), **skips `metadata.failed_update(now)`**, clears `in_progress`.

  Effect: under `metadata.recovery.strategy=NONE` (the default), a malicious or misconfigured broker sending REBOOTSTRAP_REQUIRED causes the client to stop advancing its failed-update backoff, so subsequent metadata requests fire back-to-back with no throttling. The Java client throttles them via `metadata.failedUpdate(now)`. This is exactly the divergence CLAUDE.md rule 4 forbids ("Never change the contract of public API").
- **Expected**: Add `metadata_recovery_strategy: MetadataRecoveryStrategy` as a parameter to the `MetadataUpdater::handle_successful_response` trait method (or as a field on `DefaultMetadataUpdater`, captured at construction time the way Java's inner-class captures the enclosing field). Re-introduce the gate: `if matches!(strategy, MetadataRecoveryStrategy::Rebootstrap) && is_rebootstrap_required { initiate_rebootstrap() } else if response.brokers().is_empty() { failed_update(now) } else { ... }`. Add a regression test (`Metadata::recovery_strategy=None` + REBOOTSTRAP_REQUIRED response → `metadata.failed_update` is invoked, `metadata_attempt_start_ms` unchanged).
- **Actual**: REBOOTSTRAP_REQUIRED branch always wins regardless of strategy; `metadata.failed_update` is never called on this path.
- **Disposition**: Fixed in commit `0945ba9` (`fixup! a9f854d`). Approach (b) from the expected list: captured `metadata_recovery_strategy` as a field on `DefaultMetadataUpdater` at construction (mirrors Java's inner-class field capture). Threading it through the trait method would have polluted `ManualMetadataUpdater`'s surface unnecessarily. Regression tests `handle_successful_response_rebootstrap_required_skipped_when_strategy_is_none` and `handle_successful_response_rebootstrap_required_takes_branch_when_strategy_is_rebootstrap` cover both gate dispositions.

---

## Blocking 2: `send_internal_metadata_request` UnsupportedVersion drops `handle_failed_request` → `in_progress` stays Set forever
- **File**: `src/network_client.rs:1374-1382` (the `MetadataUpdaterContext::send_internal_metadata_request` impl) + `src/network_client.rs:879-884` (the `do_send` UnsupportedVersion arm) + `src/default_metadata_updater.rs:137-142` (the caller in `maybe_update_for_node`)
- **Severity**: Blocking
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java:1340-1345` (the private `maybeUpdate(long, Node)` setting `inProgress` after `sendInternalMetadataRequest`) and Java `doSend(...)` UnsupportedVersion → `metadataUpdater.handleFailedRequest(...)`.
- **Description**: `DefaultMetadataUpdater::maybe_update_for_node` sets `self.in_progress = Some(InProgressData::new(...))` **before** calling `context.send_internal_metadata_request(...)`. Inside `send_internal_metadata_request`, `self.do_send(client_request, true, now)` runs in the take/put window where `self.metadata_updater` is `None`. If `do_send` hits the METADATA UnsupportedVersion arm at line 882-884, it tries `if let Some(updater) = self.metadata_updater.as_mut()` — which is `None` — and **silently drops** the `handle_failed_request(now, Some(e))` call. The same is true for the `builder.build(version)` failure arm at line 916-918.

  In Java, `DefaultMetadataUpdater.maybeUpdate(long, Node)` calls `sendInternalMetadataRequest`, which calls `doSend`, which calls `metadataUpdater.handleFailedRequest(now, Optional.of(ex))` on UnsupportedVersion. Java has no take/put window — the inner-class reference is live the whole time. Java's `handleFailedRequest` clears `inProgress`. So in Java, an UnsupportedVersion during the metadata-request dispatch results in `inProgress = null` and the next `maybeUpdate(now)` correctly observes no fetch-in-flight.

  In Rust, the silently-dropped `handle_failed_request` call leaves `self.in_progress = Some(InProgressData(...))` permanently set (Rust never put the request on the wire, so no `handle_completed_receives` will ever clear it). Subsequent `MetadataUpdater::maybe_update(...)` calls see `has_fetch_in_progress() = true` and add `default_request_timeout_ms` to `wait_for_metadata_fetch`. `is_update_due` returns `false` (because `has_fetch_in_progress` short-circuits). The state machine is stuck thinking a metadata fetch is in flight, and the only paths that clear `in_progress` are (a) `handle_failed_request` (never called from here) or (b) `handle_successful_response` (never called because no request was sent).

  This is the canonical "Rust ownership model loses a side effect Java's inner class preserves" trap. The actor's inline comment at network_client.rs:1374-1382 acknowledges the drop and says "just log" — but that justifies the wrong behavior: the `inProgress`-stuck state survives the silent log.
- **Expected**: Either (a) clear `in_progress` **after** the send succeeds (move the `self.in_progress = Some(...)` assignment to after `context.send_internal_metadata_request(...)` returns), and have `send_internal_metadata_request` return `Result<(), KafkaError>` so the updater can roll back on failure; or (b) buffer the `handle_failed_request` call in a side-channel on `NetworkClient` that gets drained right after `metadata_updater = Some(updater)` is restored in `poll()`. Java's invariant must hold: an UnsupportedVersion during the metadata request must clear `inProgress` so the next poll can retry.

  Also: add a regression test that exercises the exact path — `DefaultMetadataUpdater::maybe_update` against a `NetworkClient` whose `ApiVersions` pin the METADATA range to an unreachable version. Assert `in_progress` is `None` after the call. The existing `do_send_unsupported_version_internal_metadata_fires_failed_request` test (network_client.rs:2179) bypasses the take/put window by calling `do_send` directly — it does NOT pin this bug.
- **Actual**: `in_progress` remains `Some(InProgressData)` indefinitely after an UnsupportedVersion (or `builder.build`) failure inside `maybe_update`. The metadata-update state machine is permanently wedged.
- **Disposition**: Fixed in commit `0945ba9` (`fixup! a9f854d`). Approach (a) from the expected list, with three coordinated changes: (1) `maybe_update_for_node` sends first, then assigns `in_progress` (mirrors Java's literal ordering at `NetworkClient.java:1343-1344`); (2) `MetadataUpdaterContext::send_internal_metadata_request` now returns `Result<(), KafkaError>`; (3) `do_send`'s internal-METADATA UVE arms return `Err(e)` instead of trying to dispatch through the unreachable `self.metadata_updater`. The updater handles the failure locally on the `Err` arm by calling its own `handle_failed_request`. Added integration regression `maybe_update_unsupported_version_clears_in_progress` (in `network_client::tests`) plus renamed the prior direct-`do_send` test to `do_send_unsupported_version_internal_metadata_propagates_err` to reflect the new contract.

---

## Suggestion 1: Take/put-back is not panic-safe
- **File**: `src/network_client.rs:1057-1062`
- **Severity**: Suggestion (Design)
- **Java Reference**: N/A — Java has no equivalent (inner-class semantics).
- **Description**: If `updater.maybe_update(self, now)` panics for any reason (e.g. the `panic!("There are no nodes in the Kafka cluster")` at line 1288 of `MetadataUpdaterContext::least_loaded_node`, or any deeper unwinding panic from `request_builder` / `metadata.update`), the closure exits without restoring `self.metadata_updater = Some(updater);`. Subsequent `poll()` calls fail at `.expect("metadata_updater present at top of poll")` — turning a recoverable panic into an unrecoverable one for the next iteration.

  This is unlikely in production (panics propagate up the tokio task) but bites tests that `catch_unwind`, and gives a poor diagnostic.
- **Expected**: Use a `scopeguard::defer!` or RAII restoration pattern. Sketch:
  ```rust
  struct UpdaterGuard<'a, ...> { slot: &'a mut Option<M>, taken: Option<M> }
  impl Drop for UpdaterGuard<'_, ...> { fn drop(&mut self) { *self.slot = self.taken.take(); } }
  ```
  Or simply `defer!`.
- **Actual**: A panic anywhere inside `maybe_update` poisons the slot for the rest of the `NetworkClient`'s life.
- **Disposition**: Fixed in commit `c050fe8` (`fixup! a9f854d`). Implemented `UpdaterPutBackGuard` with a `*mut Option<M>` slot pointer to sidestep the borrow-checker conflict (a `&mut Option<M>` reference held by the guard could not coexist with the `&mut self` we pass into `maybe_update`). The guard's Drop fires on unwind and forces the slot to `None` (the original `M` is on the panicking stack and unrecoverable — we cannot put back the same value). The happy path calls `.disarm()` (via `mem::forget`) after re-assigning the slot. No `scopeguard` crate added — the guard is hand-rolled inline.

---

## Suggestion 2: `KafkaProducer::from_config` is `pub` rather than `pub(crate)`
- **File**: `src/producer/kafka_producer.rs:346`
- **Severity**: Suggestion (API surface)
- **Java Reference**: N/A — Java has no `KafkaProducer(ProducerConfig, Serializer, Serializer)` public constructor (only the `Map<String, Object>` form).
- **Description**: `from_config` is a Rust-only addition beyond Java's public constructor set. Its only legitimate external caller in this milestone is `tests/integration/performance_test.rs:367`, which is `#![cfg(feature = "integration-tests")]`-gated. There is no Java-API-parity reason for this method to be `pub`. Making it `pub` solidifies a non-Java public surface that future users may rely on, complicating future cleanup if Java's constructor set is preserved verbatim later.
- **Expected**: Demote to `pub(crate)`. The integration test (in the same crate's `tests/` directory) can still call it. If external callers need a "build from `ProducerConfig`" entry point, surface it as part of the `Producer` trait or as a documented Rust-only extension method.
- **Actual**: `pub fn from_config(...)` — visible to all downstream crates.
- **Disposition**: Fixed in commit `c050fe8` (`fixup! a9f854d`). Demoted to `pub(crate)` with updated rustdoc noting Java has no equivalent constructor. `tests/integration/performance_test.rs` continues to compile because it lives in the same crate.

---

## Suggestion 3: `SupportsDefaultSerializer` is `pub`; Java's `key.serializer` / `value.serializer` FQCN config keys are silently ignored on the `new()` path
- **File**: `src/producer/kafka_producer.rs:408-417`
- **Severity**: Suggestion (API surface + parity)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:283`, `kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java` (`KEY_SERIALIZER_CLASS_CONFIG`, `VALUE_SERIALIZER_CLASS_CONFIG`).
- **Description**: Java's `KafkaProducer(Map<String, Object>)` reads `key.serializer` and `value.serializer` as FQCN strings and reflectively instantiates them. The Rust no-args `new(props)` short-circuits the FQCN resolution entirely via the `SupportsDefaultSerializer` marker trait — only `Vec<u8>` is impl'd. If a user passes `key.serializer=org.apache.kafka.common.serialization.StringSerializer` in props, Rust silently uses `ByteArrayOwnedSerializer` instead. This is a behavioral deviation that the user has no way to discover at runtime.
- **Expected**: At minimum, (a) rustdoc on `new()` and `SupportsDefaultSerializer` should explicitly state "the `key.serializer` / `value.serializer` config keys are not consulted by this constructor; callers must use `with_serializers` to override"; (b) emit a `log::warn!` if `props` contains either FQCN key (similar to the partitioner-class factory at line 369-375). Optionally (c) demote the trait to `pub(crate)` since only `Vec<u8>` is impl'd in-crate.
- **Actual**: `SupportsDefaultSerializer` is `pub` (downstream crates could impl it for their own types). The `key.serializer` / `value.serializer` keys are silently ignored without diagnostic.
- **Disposition**: Fixed in commit `c050fe8` (`fixup! a9f854d`). All three remedies applied: rustdoc updated on both `new()` and the trait; `log::warn!` emitted at construction time when either config key is present (mirrors the partitioner-class warn pattern at `kafka_producer.rs:1938`); trait demoted to `pub(crate)`.

---

## Nit 1: Rustdoc claims integration tests "live in `network_client.rs::tests`" — they don't
- **File**: `src/default_metadata_updater.rs:404-408`
- **Severity**: Nit (documentation)
- **Java Reference**: `NetworkClientTest.testRebootstrap` / `testInflightRequestsDuringRebootstrap`.
- **Description**: The module-level docstring on `tests` states "Tests that *do* require the full `NetworkClient` (the `maybe_update`-driven send loop, `testRebootstrap`, `testInflightRequestsDuringRebootstrap`) live in `network_client.rs::tests` because they exercise the integrated behaviour." A `grep -n "rebootstrap" src/network_client.rs` shows the integration test is `handle_rebootstrap` (the production method), not a translated `testRebootstrap`. These tests are scheduled for Phase 8a per `Phase-8/NOTES.md` but the rustdoc claim is currently false.
- **Expected**: Change "live in" → "will land in Phase 8a in `network_client.rs::tests`". Or, since the Phase-8a integration tests will live in `tests/integration/producer_smoke_test.rs` (per `NOTES.md`), point at the correct location.
- **Actual**: Future reader is misled into searching `network_client.rs::tests` for tests that don't exist yet.
- **Disposition**: Fixed in commit `0945ba9` (`fixup! a9f854d`). Rewrote the module rustdoc to point at the Phase 8a `tests/integration/producer_smoke_test.rs` future location AND call out the Blocking-2 integration regression that lives in `network_client.rs::tests` now (`maybe_update_unsupported_version_clears_in_progress`).

---

## Nit 2: `NetworkClient::is_any_node_connecting` is genuinely dead now; rustdoc is stale
- **File**: `src/network_client.rs:803-812`
- **Severity**: Nit (cleanup)
- **Java Reference**: `NetworkClient.isAnyNodeConnecting()` — now translated as a private helper on `DefaultMetadataUpdater` using `MetadataUpdaterContext::is_connecting`.
- **Description**: The `#[allow(dead_code)]` method on `NetworkClient` was retained "for the wiring expected in Phase 6" (per the rustdoc). Phase 8.0 wired it differently — the equivalent now lives on `DefaultMetadataUpdater::is_any_node_connecting` (line 172-179). The `NetworkClient` method is no longer referenced anywhere and the docstring is stale.
- **Expected**: Either delete the method (preferred — it's now reachable only via dead code) or update the comment to "Java's `NetworkClient.isAnyNodeConnecting` is now translated on `DefaultMetadataUpdater` via the `MetadataUpdaterContext::is_connecting` callback; this method is retained for symmetry with Java's NetworkClient surface only".
- **Actual**: Dead method with misleading "Phase 6" justification still ships.
- **Disposition**: Fixed in commit `c050fe8` (`fixup! a9f854d`). Method deleted; replaced by a brief comment pointing at the new home on `DefaultMetadataUpdater`.

---

## Nit 3: `NetworkClient::metadata_updater: Option<M>` field doc references a non-existent sibling field
- **File**: `src/network_client.rs:117-120`
- **Severity**: Nit (documentation)
- **Java Reference**: N/A — the comment describes a Rust-only design.
- **Description**: The docstring on `metadata_updater: Option<M>` mentions "Optional handle to `Metadata`. Java's `DefaultMetadataUpdater` captures it as a final field; we keep a parallel Arc here so the `M` type parameter can be either `DefaultMetadataUpdater` or `ManualMetadataUpdater` without blowing up the surface." But no `metadata: Option<Arc<Metadata>>` field exists on the struct. The comment appears to be a leftover from an earlier design that was reverted. The `Arc<Metadata>` is held only inside `DefaultMetadataUpdater`.
- **Expected**: Delete the second-paragraph comment (lines 117-120), or rewrite to describe what's actually there.
- **Actual**: Future reader thinks `NetworkClient` holds a separate Metadata handle.
- **Disposition**: Fixed in commit `c050fe8` (`fixup! a9f854d`). Stale paragraph deleted.
