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

---

# Visibility correction (Phase 8a.0) — Suggestion 2/3 archive update

During Phase 8a's first attempt, Actor 8's commit `45b70a2` promoted
`DefaultMetadataUpdater` (struct + module) and `SupportsDefaultSerializer`
(trait) from `pub(crate)` to `pub` + `#[doc(hidden)]`.

Rationale:
- `KafkaProducer::with_serializers` returns
  `KafkaProducer<K, V, NetworkClient<Selector, DefaultMetadataUpdater>>`.
  Downstream crates (including `tests/integration/*`, which are
  separate downstream crates, NOT same-crate code as Critic 8's
  Phase 8.0 Round-2 archive for Suggestion 2 claimed) cannot hold
  ANY binding of a value whose type names a `pub(crate)` item.
- `#[doc(hidden)]` preserves the no-Java-API-growth intent: the types
  stay off docs.rs and are not surfaced as part of the documented public
  API.
- The earlier "Suggestion 2 fixed in `c050fe8`" disposition and
  "Suggestion 3 fixed in `c050fe8`" disposition still stand for the
  `from_config` visibility and the FQCN-key warn-log behaviour. Only
  the `pub(crate)` claim for `DefaultMetadataUpdater` /
  `SupportsDefaultSerializer` was structurally wrong; this block
  records the correction.

Files corrected:
- `src/lib.rs:35-49` — `default_metadata_updater` module
  `pub(crate) → pub` + `#[doc(hidden)]`.
- `src/default_metadata_updater.rs:77-91` — `DefaultMetadataUpdater`
  struct `pub(crate) → pub` + `#[doc(hidden)]`.
- `src/producer/kafka_producer.rs:449-463` — `SupportsDefaultSerializer`
  trait `pub(crate) → pub` + `#[doc(hidden)]`.

Disposition: Applied in commit `45b70a2`. Critic 8 will verify the
correction in their next review.

---

# Phase 8a.0 Round 1 — resolved findings

The five blocks below correspond to Critic 8's Phase 8a.0 Round 1
review (`COMMENTS.8.md` lines 368-454). All were fixed in Round 2.

## Phase 8a.0 Suggestion 1: `sender_wakeup` is a no-op — causes 30s flush latency on every clean close

- **File**: `src/producer/kafka_producer.rs:1005-1007` (definition), `src/producer/kafka_producer.rs:1463` (graceful close call site)
- **Severity**: Suggestion (real production bug; long-standing, deferred at Phase 7d; surfaced visibly by Phase 8a integration test)
- **Java Reference**: `KafkaProducer.java:1129` (`sender.wakeup()` during `waitOnMetadata`), `NetworkClient.java:1325-1326` (`wakeup()` → `selector.wakeup()`), `Sender.java:298` (close path)
- **Description**: This is the root cause of the "30.005s, 30.007s, 30.005s" close-drain timing the actor flagged. The `KafkaProducer::send()` path appends to the accumulator (via `do_send` → `accumulator.append`) and then calls `sender_wakeup()` which **is documented as a no-op since Phase 7d**. The actor's rustdoc at `kafka_producer.rs:995-1002` correctly identifies the consequence: "A missed wake-up degrades first-send latency by at most one Sender tick (`linger.ms` + `request.timeout.ms`)". With `request.timeout.ms = 30000` (default), that is exactly the observed 30s clustering.
- **Expected**: Add a real wake mechanism. Per the actor's own rustdoc, the two viable shapes are `Arc<dyn Fn() + Send + Sync>` extracted pre-spawn or a `tokio::sync::Notify` plus a `select!` arm in the Sender's `run_loop`. The latter is simpler — add a `Notify` field to `Sender`, replace `sender_wakeup`'s no-op body with `notify.notify_one()`, and add a `Notify::notified()` arm to the Selector's poll `select!` (or to the Sender's `run_once` outermost await). Java's `selector.wakeup()` is exactly this primitive.
- **Actual**: Every clean close on a healthy broker paid a ~30s delay whenever the Sender was mid-poll at the moment of close. In long-running producers this also degraded first-send latency after an idle window.
- **Disposition**: Fixed in commit `397dc09` (`fixup! 92f79af`). Approach (b) from the expected list — `tokio::sync::Notify` on `Selector` with a new arm in `Selector::poll`'s `tokio::select!`. `KafkaProducer` holds an `Option<Arc<Notify>>` extracted pre-spawn from the Selector via `Selector::wakeup_notify_handle()`. `Notify::notified` is documented cancellation-safe per CLAUDE.md rule 9.6. Manual integration measurement: close drained in **2.697 ms** for 50 small records on localhost (vs. ~30 s pre-fix). Three consecutive Round-2 verification runs measured close drains of 4.2 ms / 2.2 ms / 3.6 ms — all well under the new 5 s `CLOSE_TIMEOUT` (see test-tightening below).

## Phase 8a.0 Suggestion 2: `default.request.timeout.ms` cap in `NetworkClient::poll` is **the** wake-up backstop — single point of failure

- **File**: `src/network_client.rs:1153`
- **Severity**: Suggestion (defense-in-depth)
- **Java Reference**: `NetworkClient.java` (`poll`)
- **Description**: `effective_timeout = timeout_ms.min(metadata_timeout).min(self.default_request_timeout_ms as i64)`. This 30s cap was, at the time of review, the **only** thing bounding Sender wake-up latency when (a) `sender_wakeup` was a no-op (Suggestion 1) and (b) there were no in-flight requests for the read-readiness wake to fire on. If a future refactor raised `default.request.timeout.ms` or removed this `.min()` cap because it looked redundant, every clean close would become unbounded by `i64::MAX`.
- **Expected**: Once Suggestion 1 lands, leave this cap as-is but add a one-line comment "this is a backstop; the load-bearing wake is `Notify` via `sender_wakeup`".
- **Actual**: Cap was undocumented and load-bearing.
- **Disposition**: Fixed in commit `fec6b0a` (`fixup! 480d304`). Rustdoc-only. With Suggestion 1 landed in `397dc09`, the cap is now belt-and-suspenders — the load-bearing wake is the `Notify` chain. The new comment explicitly calls out the regression risk to future readers ("Do not remove this `.min()` even if it looks redundant — it is the floor that protects the close-drain contract from a missed-wake regression"). Expanded one line beyond Critic 8's expected minimum to also describe the mock-injected-client edge case where `sender_wakeup_notify` is `None`.

## Phase 8a.0 Suggestion 3: No lib-level regression test pins the wake-on-read fix

- **File**: `src/common/network/selector.rs:1192-1239` (new `wait_any_transport_readable`)
- **Severity**: Suggestion (test coverage)
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/common/network/SelectorTest.java` (existing echo-server based tests cover this transitively in Java)
- **Description**: The wake-on-read fix is the load-bearing Phase 8a.0 production change. It was exercised end-to-end by `producer_smoke_plaintext_1000_records` (which requires Docker) but had no dedicated lib-level test. The existing Selector echo-server tests are tight enough loops that the previous bug (sleep-for-full-timeout) was masked — they all use short timeouts like `poll(0)` or wait-for inside a `wait_for` helper.
- **Expected**: Add a `#[tokio::test]` in `src/common/network/selector.rs::tests` that: (1) connects two channels to the echo server, (2) calls `poll(5000)` (a 5s ceiling), (3) from another tokio task sends bytes on the underlying server socket so the kernel makes the client socket readable, (4) asserts `poll` returns within e.g. 100ms (well under the 5s timeout).
- **Actual**: Wake-on-read was unverified at lib-test level.
- **Disposition**: Fixed in commit `edcd5bc`. Two regression tests added in `selector.rs::tests`: (a) `poll_wakes_when_socket_becomes_readable` pins the wake-on-read fix from `480d304` using an `EchoServer` + connected channel + an outer `tokio::time::timeout(1_000ms, ...)` fast-fail wrapper, asserting `poll(5000)` returns in < 500 ms and the echoed payload lands in `completed_receives`; (b) `poll_wakes_when_notify_one_is_called` pins the Notify wake from `397dc09` using a sibling task that calls `notify_one()` after a 20 ms delay, asserting `poll(5000)` returns in < 200 ms. Both tests use generous bounds (real wake fires in microseconds) and outer-timeout fast-fail wrappers so a regression fails the test in 1-2 s instead of the full 5 s `poll` timeout. Test count: 1222 → 1224.

## Phase 8a.0 Nit 1: Request hex-fixture documentation overclaims "captured live from broker"

- **File**: `src/common/requests/api_versions_request.rs:288-308` (rustdoc on `hex_fixture_api_versions_request_v4_apache_kafka_4_2`)
- **Severity**: Nit (test documentation precision)
- **Java Reference**: PLAN.md Risk #1
- **Description**: The fixture's rustdoc said bytes were "captured live during the `producer_smoke_plaintext_1000_records` integration test, off an Apache Kafka 4.2.0 broker that successfully decoded the request". True but read as if the **broker** emitted these bytes. They are **Rust-emitted bytes accepted by a Java broker** — a weaker invariant than "Java-emitted bytes that the Rust client must parse". The response fixture in the sibling file IS broker-emitted, no concern. PLAN.md Risk #1 specifies "capture hex fixtures from the Java client" which the response fixture satisfies; the request fixture proves wire-compat by acceptance.
- **Expected**: Rewrite the first paragraph of the rustdoc to say "Bytes are the request payload the Rust client emits for the documented inputs, verified by Apache Kafka 4.2.0 accepting and successfully replying. Wire-compatibility-by-acceptance, not byte-for-byte match against Java's `KafkaProducer` emission."
- **Actual**: Reader could conclude that broker emitted these request bytes.
- **Disposition**: Fixed in commit `f568032` (`fixup! 30b2bc2`). Rustdoc-only. First paragraph rewritten to explicitly call out the fixture asymmetry: request fixture is Rust-emitted + broker-accepted (wire-compatibility-by-broker-acceptance), response fixture in `api_versions_response.rs` IS Java/broker-emitted (the stronger invariant on the response-parse path). Matches Critic 8's exact phrasing target.

## Phase 8a.0 Nit 2: Stale rustdoc reference to `wait_any_channel_readable` (function is named `wait_any_transport_readable`)

- **File**: `src/common/network/selector.rs:1004`
- **Severity**: Nit (documentation drift)
- **Java Reference**: N/A — Rust-only doc
- **Description**: The `SAFETY:` block at line 1003-1006 mentioned `wait_any_channel_readable` which does not exist. The actual function is `wait_any_transport_readable`. Probably an earlier draft name.
- **Expected**: Rename in the comment.
- **Actual**: Future reader would grep for `wait_any_channel_readable` and find nothing.
- **Disposition**: Fixed in commit `397dc09` (`fixup! 92f79af`) — folded into Suggestion 1's commit as a drive-by. The Suggestion-1 commit message explicitly mentions it under "Updated:". Verified with `grep -rn "wait_any_channel_readable" src/` → no matches.

## Phase 8a.0 close-flush watchdog tightening (Critic 8 hand-off note)

- **File**: `tests/integration/producer_smoke_test.rs:434` — `CLOSE_TIMEOUT`
- **Severity**: Hand-off (test contract tightening)
- **Description**: Critic 8 Phase 8a.0 Round 1 hand-off note read: "the 90s close-timeout in the test is a watchdog, not the contract. If Suggestion 1 lands and `sender_wakeup` becomes a real wake, the close path should drain in <1s on localhost — tighten the assertion bound at that point so future regressions in the wake mechanism are caught by the test."
- **Disposition**: Fixed in commit `b4685d1` (`fixup! 6a2014e`). `CLOSE_TIMEOUT` tightened from 90 s to 5 s. Manual run after Suggestion 1 landed: close drained in **2.697 ms** for 50 small records on localhost. Three consecutive Round-2 verification runs measured 4.2 ms / 2.2 ms / 3.6 ms. 5 s is ~1000× the post-fix drain time — generous watchdog, still tight enough to catch a future missed-wake regression. Strict-less assertion `close_elapsed < CLOSE_TIMEOUT` preserved.

---

# Visibility correction (Phase 8a.1) — `from_config` archive update

`tests/integration/performance_test.rs` was muted in Phase 1 cleanup
and re-enabled in Phase 8a.1 against the Phase 7g
`Result<KafkaFuture<RecordMetadata>, KafkaError>` send shape. The
perf test is the only legitimate external caller of
`KafkaProducer::from_config` (it builds a producer from a
pre-validated `ProducerConfig` instead of re-stringifying through a
`HashMap`).

The previous Critic 8 "Suggestion 2" disposition (commit `c050fe8`)
demoted `from_config` from `pub` → `pub(crate)` on the premise that
the only caller was same-crate test code. That premise was
structurally wrong by the same Phase 8a.0 reasoning that promoted
`DefaultMetadataUpdater` / `SupportsDefaultSerializer`:
`tests/integration/*` is a downstream crate, NOT same-crate code.
A `pub(crate)` `from_config` makes the perf test uncompilable.

Rationale (same as Phase 8a.0):
- `tests/integration/performance_test.rs` cannot reach a `pub(crate)`
  constructor at all — it is a separate downstream crate of
  `confluent-kafka-rust`.
- `#[doc(hidden)]` preserves the no-Java-API-growth intent: the
  constructor stays off docs.rs and is not surfaced as part of the
  documented public API.
- External callers should still prefer `with_serializers` (parses a
  raw `HashMap<String, String>`) or `new` (no-args byte vector
  default). `from_config` is the pre-validated `ProducerConfig`
  shortcut — useful for perf tests and any caller assembling config
  programmatically.

Files corrected:
- `src/producer/kafka_producer.rs:409` — `from_config`
  `pub(crate) → pub` + `#[doc(hidden)]`. Rustdoc updated to call out
  the visibility forcing function and pin the `#[doc(hidden)]`
  rationale.

Disposition: Applied in commit `3ddf99e` (`fixup! db0a1b8`).

Smoke-run verification (`NUM_MESSAGES=1000 WARMUP_SECONDS=2
TEST_DURATION_SECONDS=5 cargo test --features integration-tests
performance_test --release`): 1000 messages sent / 1000 completed /
0 errors / 997.68 msg/s throughput / 0.98 MiB/s / `producer.send()`
median <5 us / metrics file written. Test runs to completion.

---

## Phase 8a Round 1 — accept-with-followups (closed by Manager)

Review window: commits `6d722af..c2347b2` on branch `fresh-impl` (32 commits, spanning sub-phases 8a → 8a.0 → 8a.1 → 8a.2 + housekeeping).

Round 1 Critic verdict: **0 Blocking, 2 Suggestion, 1 Nit. accept-with-followups.** Full Round 1 review remains at `COMMENTS.8.md:466-549` as the audit trail for this archive entry.

### Suggestion 1 — RESOLVED in NOTES.md

NOTES.md DoD #3 amended by Manager to make the per-sub-phase incremental schedule explicit, matching the sub-phase table:
- **8a onward**: Ack count + `RecordMetadata` shape (topic, partition range, non-negative offset, non-`-1` timestamp).
- **8b onward**: Partition consistency (requires partitioner-result accessor) + per-partition monotonic offsets.
- **8c onward**: End-to-end byte fidelity.

Resolution rationale: the sub-phase table already reads "8b: partition consistency + monotonic-offset"; the DoD #3 list was the source of truth ambiguity. Amending DoD #3 to qualify each item with its sub-phase scope removes the conflict without changing the actual incremental plan. Disposition: NOTES.md commit (housekeeping + clarification rollup, see commit below).

### Suggestion 2 — DEFERRED to Phase 8b

`close_flushes_pending_inflight` (`producer_smoke_test.rs:480-496`) currently asserts only `offset >= 0`. Phase 8b expands the test set with per-partition monotonic-offset and partition-consistency asserts; the same expansion will bring test 2 to parity with test 1's full shape check (topic match, partition range, non-`-1` timestamp). Tracked as a sub-task of Phase 8b's test expansion; no separate fixup commit.

### Nit 1 — DEFERRED to Phase 8b

Rustdoc on `close_flushes_pending_inflight` (`producer_smoke_test.rs:441-447`) still reads "close drains in microseconds — milliseconds at worst". Observed wall-clock is 2.5–3.2 ms. The Phase-8a.0 Round-2 archive already corrected the framing in the rustdoc preamble; the integration test's local comment didn't propagate. Bundled with Suggestion 2's test-tightening edit in Phase 8b.

### Verifications retained from Round 1

- `cargo build --features integration-tests` clean (~22 s).
- `cargo xtask format-check` green.
- `cargo xtask lint` green, no warnings.
- `cargo test --features integration-tests producer_smoke -- --nocapture`: 2/2 green, 8.06 s.
- Lib tests 1220 → 1233 (+13). Integration tests +2.
- CLAUDE.md §11 hot-path audit: no new `String` clones / `Box<dyn Future>` per send / `tokio::spawn` per message in production code.
- CLAUDE.md §12 zero-copy: 8a.2's `Arc<AtomicBool>` `SendCompletion` does not copy buffer bytes; payload remains in a single owned `Bytes` chain.
- CLAUDE.md §9 concurrency: no new `MutexGuard` held across `.await`.
- `#[doc(hidden)]` cordon: `DefaultMetadataUpdater`, `SupportsDefaultSerializer`, `KafkaProducer::from_config` all marked + rustdoc-explained + not `pub use`-re-exported.

Phase 8a Round 1 closes. Manager advances to Phase 8b plan.
