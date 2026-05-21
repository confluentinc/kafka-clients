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
## Phase 8b Round 1 — Critic review

Review window: commits `7d9f892..275eb3b` on branch `fresh-impl` (4 commits, all by Actor 8 on 2026-05-20).

Java references consulted:
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:950` — `interceptors.onSend(record)` runs **before** partitioning at line 1024.
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:1014-1024` — explicit-partition honored branch in `KafkaProducer.partition()`.
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:1036-1051` — `accumulator.append` → `assert appendCallbacks.getPartition() != UNKNOWN_PARTITION` → wake-up. The Rust observation point at `kafka_producer.rs:1471-1486` sits exactly between the assertion (line 1038 Java) and the transaction-manager branch (line 1044 Java) — Java-faithful lifecycle placement.
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:1600` — `interceptors.onAcknowledgement` runs post-broker-ack; cannot observe pre-network partition. Confirms the actor's "no pre-network interceptor observation point" rationale.

Verdict: **0 Blocking, 0 Suggestion, 1 Nit.**

---

### Per-area verifications

| Area | Status | Notes |
|------|--------|-------|
| Build (`cargo build --features integration-tests`) | OK | Clean, 12.10 s. |
| Format-check (`cargo xtask format-check`) | OK | Green. |
| Lint (`cargo xtask lint`) | OK | Green, no warnings. |
| Lib tests (`cargo test --lib`) | OK | 1233 passed, no count change vs. 8a close (production surface unchanged outside cfg-gated seam). |
| Integration tests (`cargo test --features integration-tests producer_smoke`) | OK | 4/4 green against real broker in 9.01 s (Testcontainers run from this review session, not Actor's reported figure). |
| Observation-point Java parity | OK | Site in `kafka_producer.rs:1471-1486` sits between Java line 1038 (post-`append`, post-assert) and Java line 1044 (transaction-manager). Fires **exactly once per `do_send`** — confirmed by `grep`: only one call site for `do_send_inner` (`kafka_producer.rs:1321`), one for `do_send` (`kafka_producer.rs:1930`). Sender retries operate on already-batched records and never re-enter `do_send_inner`. CLAUDE.md rule 9.5 (callback-obligation) satisfied. |
| Observer fires only on success path | OK | Located **after** `accumulator.append().await?` — propagated errors short-circuit before the observer. Matches the test's `observed.len() == HAPPY_PATH_RECORDS` invariant (records that error out before reaching `append` also error their futures, so the test's `unwrap_or_else(panic)` catches them too — no silent skew). |
| `partition_observer` cfg-gating | OK | All 6 references (`grep -n partition_observer src/producer/kafka_producer.rs`) are inside `#[cfg(any(test, feature = "integration-tests"))]` blocks: type alias (144), field decl (298), `set_partition_observer` impl (344-352), constructor initializer (990-991), observation site (1477-1486). Field literally does not exist in release builds without the feature. |
| Lock-across-`.await` (CLAUDE.md 9.6) | OK | Observer is `Fn(&str, i32) + Send + Sync` (sync). At the observation site (1479-1486) the actor extracts `Option<Arc<...>>` from the guard, drops the guard via inner-scope, then invokes the closure. `set_partition_observer` itself does not `.await`. |
| Hot-path allocation audit (DoD #10) | OK | `git diff 40beb4a..275eb3b -- src/` shows zero new String clones, no `Box<dyn Future>` per send, no per-message `tokio::spawn` in production code. The only `Arc::new` in production diff is inside `set_partition_observer` (test-only, called at most once per test). |
| Test 1 (explicit-partition) — coverage assertion | OK | Tightened from `>=2` to exact `3` partitions seen. Explicit-partition path makes this deterministic by construction. |
| Test 1 — per-partition monotonic offsets | OK | `windows(2)` strict-monotonic check over per-partition offset vectors. Sequential `send().await` keeps per-partition send order deterministic. |
| Test 2 (auto-partition) — observer-vs-broker agreement | OK | `observed[i] == metadatas[i].partition()` per record. The invariant holds because (a) `send().await` returns *after* `do_send_inner` returns *after* the observer fires, so `observed` accumulates in send order; (b) `join_all` preserves input ordering, so `metadatas[i]` is the i-th sent record's metadata regardless of broker ack order across partitions. |
| Test 2 — exactly-once observer assertion | OK | `observed.len() == HAPPY_PATH_RECORDS` (line 553-559) pins the CLAUDE.md rule 9.5 callback contract: double-fire on a Sender retry would exceed, under-fire from a bypassed path would fall short. |
| Test 2 — sticky-partitioner caveat | OK | Rustdoc lines 435-444 explicitly explain why 3-partition coverage is **not** asserted here (sticky partitioner can collapse a 1000-record burst into one partition in a single linger window) and points the reader to test 1 for the coverage contract. Honest framing. |
| Test 3 (`flush_drains_50_records_through_public_api`) | OK | Routes through `Producer::flush().await` (line 698) — the public-API path, not the accumulator-direct shortcut Phase 7e was forced into. Captured-future-then-flush-then-await pattern correctly exercises flush's drain contract. |
| Test 3 — post-flush usability | OK | Lines 773-786 send + ack one additional record after `flush()`. Catches the close-vs-flush regression (a regression that turned `flush()` into a `close()` would pass assertions 1-3 — captured futures would still resolve — but the post-flush send would fail with `IllegalState`). |
| Test 3 — Phase-7 carry-over #2 retired | OK | NOTES.md "Phase-7 carry-overs retired here" #2 reads "50-record `flush()` fidelity — 8b's per-partition multi-record drain exercises `flush` through `producer.send()`, not the accumulator-direct shortcut Phase 7e was forced into". Test 3 matches this brief verbatim. |
| Test 4 (`close_flushes_pending_inflight`) — 8a Suggestion 2 followup | OK | Per-record shape loop at lines 959-969 now applies topic match, partition range `[0, 3)`, `offset >= 0`, and `has_timestamp()`. Matches test 1's contract. Catches synthetic-`RecordMetadata` regression in graceful-close path. |
| Test 4 — 8a Nit 1 followup (rustdoc accuracy) | OK | Lines 892-903 separate the wake primitive (`Notify::notify_one()` CAS, microseconds) from end-to-end close-drain (low-millisecond range, broker-ack-RTT-dominated, observed 2-4 ms). Matches the Round-2 archive framing at `COMMENTS.8.md:446-450`. |
| NOTES.md Phase 8b stanza | OK | Appended at `NOTES.md:126-196`. Documents the 4 commits, the landed assertions (8b "onward" partition consistency + per-partition monotonic offsets, Phase-7 carry-over #2, 8a Suggestion-2 / Nit-1), what's deferred to 8c+ (byte fidelity, compression matrix, TLS, 3-consecutive-run gate). Accurate against the diff. |
| NOTES.md DoD #3 — partition consistency (8b) | OK | Explicit path: test 1 (line 359-364) — `m.partition() == i % TOPIC_PARTITIONS` per record. Auto path: test 2 (line 581-596) — observer-vs-broker agreement per record. **Both** sides of "partition consistency" pinned, which is stronger than the bare DoD requirement. |
| NOTES.md DoD #3 — per-partition monotonic offsets (8b) | OK | Tests 1 and 3 both apply `windows(2)` strict-monotonic over per-partition offset vectors. |
| NOTES.md DoD #4 — no new String clone / Box<dyn Future> / per-message spawn | OK | Confirmed by `git diff 40beb4a..275eb3b -- src/`: zero matches for `String::|to_string|to_owned|Box<dyn Future|tokio::spawn` in production diff outside the cfg-gated test seam. |
| `kafka` submodule status (`modified: kafka (untracked content)`) | OK | Untracked `bin/` build artifacts inside the Java source tree, not in review window. |

---

### Nit 1: `KafkaProducer` struct rustdoc visually trails into the `PartitionObserverFn` type-alias rustdoc

- **File**: `src/producer/kafka_producer.rs:125-144`
- **Severity**: Nit
- **Description**: Lines 125-138 are the `KafkaProducer` struct's rustdoc; line 139 is blank; lines 140-142 are three `///` lines that document `PartitionObserverFn` (the cfg-gated type alias at 143-144). Rust's doc-comment grouping correctly attaches lines 140-142 to the type alias (because of the blank line separator at 139). Functionally fine. But a reader skimming the file sees four consecutive doc-comment blocks under one heading-like `[`Producer`]: ...` reference link, which suggests the `PartitionObserverFn` text belongs to `KafkaProducer`. A `//` (non-doc) separator comment or a blank-line + the existing `#[cfg]` form on its own visual block would make the grouping obvious.
- **Expected**: Either move the `PartitionObserverFn` rustdoc to sit immediately above its own `type` decl with no preceding `[`Producer`]:` link, or insert a short `// ---- Test seam type aliases ----` separator comment between line 138 and line 140 to break up the visual block. Doc-comment text could also state explicitly "This is a top-level type alias, not a field of `KafkaProducer`."
- **Recommendation**: Nit. Documentation readability only; the compiler and rustdoc generator both group correctly. Optional polish; can be deferred indefinitely or bundled with the next touch on the file.

---

### Round 1 verdict: **0 Blocking, 0 Suggestion, 1 Nit. accept-with-followups for close.**

The four commits land Phase 8b's DoD #3 "8b onward" assertions cleanly and retire both the Phase-7 carry-over #2 (50-record `flush()` fidelity) and the two 8a Round 1 follow-ups (Suggestion 2, Nit 1) flagged earlier in this file. Every gate is green: build, format-check, lint, 1233 lib tests, 4/4 integration smoke tests against a real Testcontainers broker.

The `partition_observer` test seam is well-designed:
- Cfg-gated so the production hot path is unaffected (release builds without `integration-tests` feature do not include the field at all).
- Lifecycle placement is Java-faithful (between `accumulator.append` return and the transaction-manager branch — matches Java lines 1038-1044).
- Callback fires exactly once per `do_send` (CLAUDE.md rule 9.5).
- Lock acquire is sync, guard is dropped before the closure body runs (no lock-across-`.await` per CLAUDE.md 9.6).
- The Rust-only addition is justified — Java's `onSend` (parity with Rust's `on_send`) runs before partitioning, and Java's `onAcknowledgement` runs after the broker RTT, so neither interceptor end can independently witness the pre-network partition. The actor verified this against `KafkaProducer.java:950` and `KafkaProducer.java:1600`.

Test 2's exactly-once observer assertion is a strong addition — it doubles as a callback-contract pin (Sender-driven retries do not re-enter `do_send_inner`). The sticky-partitioner caveat (test 2 deliberately does not assert 3-partition coverage) is correctly documented and the coverage contract is upheld by test 1's explicit-partition path.

Test 3's post-flush-usability assertion (lines 773-786) is exactly the right pin for distinguishing `flush()` from `close()` — a regression that turned `flush()` into a `close()` would have passed assertions 1-3 but failed the post-flush send. This is the kind of regression-catching assertion that DoD #3 should encourage.

**No production-code defects observed.** The only finding is a Nit on doc-comment grouping at `kafka_producer.rs:125-144` — a purely visual concern that does not affect generated rustdoc or compiler behavior.

### Next steps for Manager

- **Phase 8b can close as accept-with-followups (or as a straight accept).** Zero Blocking, zero Suggestion. The Nit is documentation polish that does not block close.
- The NOTES.md Phase 8b stanza is accurate and complete; no Manager amendment needed.
- Phase 8c (end-to-end byte fidelity) can begin without prerequisite cleanup from 8b.



Phase 8b Round 1 closes. Manager advances to Phase 8c (end-to-end byte fidelity) plan.
## Phase 8c Round 1 — Critic review

Review window: commits `82f3254..07a207d` on branch `fresh-impl` (4 commits — harness helper, end-to-end byte-fidelity test, doc-grouping fix, NOTES.md close stanza). Manager-housekeeping commit `4802cdb` is agent-memory archive only and excluded.

Java references consulted:
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:1330-1390` — `close(Duration)` contract that the test's graceful-close-before-consume mirrors.

Verdict: **0 Blocking, 1 Suggestion, 2 Nit.**

---

### Per-area verifications

| Area | Status | Notes |
|------|--------|-------|
| Production diff scope | OK | `git diff 81a2607..07a207d -- src/` is **only** the doc-grouping fix at `src/producer/kafka_producer.rs:125-144`. No other production change. |
| Doc-grouping fix is a real bug fix (not just nit) | OK — **8b Round 1 review was wrong** | See "Resolution of the 8b Round 1 doc-grouping claim" below. Re-verified by rustdoc generation: pre-fix `struct.KafkaProducer.html` contains **none** of the doc text; post-fix it does. |
| Test seam (`PartitionObserverFn`) still compiles + still wires the test seam | OK | `cargo test --features integration-tests producer_smoke -- --test-threads=1` passes all 5 tests; the type alias is referenced in test code unchanged. |
| Format-check | OK | `cargo xtask format-check` green. |
| Lint | OK | `cargo xtask lint` green, no warnings. |
| Lib-test count regression | OK | `cargo test --lib`: **1233 passed** (no count change — production diff is comment/whitespace + a type-alias relocation, exactly as the close stanza claims). |
| Integration suite | OK | `cargo test --features integration-tests --test integration producer_smoke -- --test-threads=1`: **5/5 green**, 21.70 s wall-clock end-to-end. (Actor's quoted 14.83 s likely measures a warm-Docker re-run; my 21.70 s is a cold-Docker first invocation. Either way well inside any reasonable per-run budget for the 8f 3-consecutive-run gate.) |
| New test pins the right invariant | OK | `producer_smoke_plaintext_byte_fidelity` asserts the consume-side `(key, value)` byte sequence equals the produce-side sequence **per partition** (`producer_smoke_test.rs:1351-1372`). Bytes — not strings (the lossy UTF-8 decode is identity for the ASCII fixtures, and the asserted comparison is on `Vec<u8>`). Per-partition grouping uses the **ack** partition (`m.partition()`), matching the brief's request that grouping survive an explicit-vs-ack-partition divergence. |
| Per-partition coverage | OK | 100 records × explicit partition `i % 3` → 33-34 records per partition. All 3 partitions exercised deterministically. |
| Helper parser robustness | OK | `splitn(3, "\u{1F}")` produces exactly 3 parts; `Partition:` prefix stripping with explicit `unwrap_or_else(|| panic!(..))` on every parse boundary. Failure mode of an unexpected stdout line (deprecation warning, broker banner) is a loud panic with full stderr captured — not a silent miscompare. |
| `--formatter-property` rationale | OK — verified | Switching from `--property` to `--formatter-property` is a real Kafka 3.7+ deprecation. The bundled broker is recent enough that `--property` would print a deprecation warning to stdout, which `lines()` would parse as a record and panic on "missing `Partition:` prefix" — the switch is necessary, not stylistic. |
| Format string verification | OK | `Partition:<n>\x1F<key>\x1F<value>\n` matches `DefaultMessageFormatter`'s actual print path (`Partition:` field joined to the rest by `key.separator`, then key and value joined by `key.separator`). The Actor's note that the brief's `\t` assumption was wrong is correct. |
| Byte-fidelity caveat documented | OK | `producer_smoke_test.rs:219-238` rustdoc explicitly states: "kafka-console-consumer defaults to the **string** key/value deserializers, which lossily decode bytes as UTF-8 (invalid sequences become U+FFFD). Phase 8c's test fixtures use only ASCII keys and values … so the lossy decode is the identity function". Future non-ASCII fixtures route is documented (`--formatter-property key.deserializer=ByteArrayDeserializer`). |
| Async-context discipline | OK | `consume_records` is sync (mirrors `create_topic`); the async test wraps it in `tokio::task::spawn_blocking` (`producer_smoke_test.rs:1298-1302`) so the `docker exec` call cannot stall the runtime. |
| Graceful close before consume | OK | Pattern: await all acks → close gracefully → then consume. Close-before-consume is belt-and-braces (acks already guarantee broker commit), but it correctly hardens against a future regression that returned acks early (e.g. `acks=1` with a post-ack linger window). |
| NOTES.md sub-phase 8c row consistency | OK | `NOTES.md:20` ("End-to-end byte fidelity: consume produced batch via `kafka-console-consumer` (`docker exec`) and assert key/value bytes match per partition") matches what landed. |
| NOTES.md close stanza accuracy | OK | The four-commit summary matches the actual commits; the "0 lib-test count change" claim is verified; the "5-test integration suite green" claim is verified. |
| Deferred-followup tracking | Manager housekeeping | The "Deferred followups carried into Phase 8c" section (`COMMENTS.8.md:553-555`) is now obsolete and additionally misclaims "no compiler/rustdoc effect" — verifiably wrong (see Resolution below). Manager to clean up; not Actor's job. |
| DoD #3 "8c onward — End-to-end byte fidelity" | OK | The DoD line at `NOTES.md:94-95` is satisfied by `producer_smoke_plaintext_byte_fidelity`. |
| DoD #4 hot-path allocation audit | OK | Production diff is exclusively a doc-comment / type-alias relocation. No new `String` clone, no `Box<dyn Future>` per send, no per-message `tokio::spawn` introduced. |

---

### Resolution of the 8b Round 1 doc-grouping claim

**My Phase 8b Round 1 review (`COMMENTS.8.md:603`, archived at `COMMENTS.DONE.8.md` post-archive) was wrong.** I wrote: "Rust's doc-comment grouping correctly attaches lines 140-142 to the type alias (because of the blank line separator at 139). Functionally fine." Both the mechanism and the inferred consequence were incorrect.

Empirical re-verification (with the pre-fix source at `git show 275eb3b:src/producer/kafka_producer.rs`):

1. **Mechanism**: there was no blank-line separator. Lines `/// A Kafka client...` (123) through `/// to satisfy clippy::type_complexity.` (142) form a contiguous `///` block (line 139 is `/// [`Producer`]: crate::producer::Producer`, a doc line, not blank). In Rust, contiguous `///` plus any `#[cfg(...)]` attribute that follows all attach to the **next syntactic item** — here, `type PartitionObserverFn`. There is no blank-line-separator rule that re-targets doc comments to a later item.

2. **Consequence (verified by rustdoc generation on a minimal repro at `/tmp/doctest/`)**:
   - With `--features integration-tests`: `type.PartitionObserverFn.html` contained the entire 22-line `KafkaProducer` doc block. `struct.KafkaProducer.html` had **zero rustdoc** (no `top-doc` section).
   - Without the feature: the cfg-gated type alias was excluded from the doc build entirely, so the docs vanished completely — `struct.KafkaProducer.html` still had no rustdoc.

   The struct's documentation on docs.rs was silently empty in both feature configurations.

3. The Actor's claim that clippy's `empty_line_after_doc_comments` lint flags this layout did **not** reproduce in my local runs (`cargo xtask lint` green on pre-fix state). The lint requires a blank line between the `///` block and the next attribute/item, which this case lacked. So clippy did not catch it — but rustdoc's silent re-attachment did do the damage the Actor describes. The fix is correct regardless of whether clippy fired.

The Actor's commit message slightly overstates clippy's role (the lint did not fire on the original layout), but the bug-fix substance is verified real.

---

### Findings

#### Suggestion 1 — `Arc::try_unwrap` ceremony in the test's close path is unnecessary

- **File**: `tests/integration/producer_smoke_test.rs:1280-1288`
- **Severity**: Suggestion
- **Description**: `KafkaProducer::close_with_timeout` is declared `async fn close_with_timeout(&self, timeout: Duration) -> Result<…>` (`src/producer/kafka_producer.rs:2020`). It takes `&self`, not `self`, so owned access is not required. The test does:

  ```rust
  let producer_for_close = producer.clone();   // Arc::strong_count = 2
  drop(producer);                                // strong_count = 1
  let producer_for_close = Arc::try_unwrap(producer_for_close)
      .map_err(|_| ())
      .expect("producer Arc had outstanding refs at close");
  producer_for_close.close_with_timeout(Duration::from_secs(30)).await…
  ```

  The `clone` → `drop` → `try_unwrap` triplet adds a panic surface (`try_unwrap` panics if any Arc leak exists — currently none, but a future background task that captures `Arc<KafkaProducer>` would silently break this test rather than the production path it audits). The simpler `producer.close_with_timeout(…).await` on the `Arc<KafkaProducer>` directly is equivalent and panic-free.
- **Verified safe**: I grepped `src/producer/kafka_producer.rs` for `Arc<Self>` / `Arc<KafkaProducer>` / `self: Arc<` and found no matches. No background task holds an `Arc<KafkaProducer>` today, so `try_unwrap` does succeed — but the protection it provides is illusory (the test's invariant is "close + then consume", not "no Arc leaks", and the latter is not part of the Phase 8c contract).
- **Expected**: Replace the three lines with `producer.close_with_timeout(Duration::from_secs(30)).await…` and remove the manual `drop` (the `Arc` will drop naturally at end-of-scope). Net: one line, no panic surface.

#### Nit 1 — Helper rustdoc claim "the parsing below would still work as long as the separator (`\x1F`) does not collide with any byte in the payload" is one bit stronger than the parser actually guarantees

- **File**: `tests/integration/producer_smoke_test.rs:233-238`
- **Severity**: Nit
- **Description**: The rustdoc says the parser would still work for binary payloads "as long as the separator (`\x1F`, ASCII unit separator) does not collide with any byte in the payload". That is necessary but not sufficient: the parser also calls `stdout.lines()` (line 317), which splits on `\n` (0x0A). A binary payload containing `0x0A` (newline) would split a single record across two output lines and the per-line parser would panic on "missing `Partition:` prefix" on the orphan second line. The current ASCII fixtures avoid this trivially, so it's a no-op for Phase 8c — but a future caller reading this rustdoc as guidance for binary payload work would also need to address newline collisions, not only `\x1F` collisions.
- **Expected**: When extending the helper for binary payloads, also document the `\n` collision: callers must either guarantee `\n` is absent from payloads, or switch to a length-prefixed framing (e.g. `--formatter-property line.separator=…`) — separator-byte management is the dominant correctness axis, not the only one.

#### Nit 2 — Commit `bc0508d` message overstates clippy's role in surfacing the bug

- **File**: commit `bc0508d` message body (not in source)
- **Severity**: Nit
- **Description**: The commit message says "clippy's `empty_line_after_doc_comments` lint explicitly notes 'the comment documents this type alias'". I could not reproduce a clippy warning on the pre-fix source (`cargo xtask lint` green; `cargo clippy --features integration-tests --lib -- -W clippy::empty_line_after_doc_comments` silent). The lint's documented behavior requires a *blank line* between the `///` block and the next item, which the pre-fix code did not have. The bug-fix substance is real (rustdoc generation pins it; see Resolution above), but clippy did not actually catch it in this codebase configuration.
- **Expected**: No source-code change. If the Actor consults their captured clippy output and confirms a different lint name fired, that's fine — but the message as written attributes credit to a lint that does not fire on this input pattern.

---

### Round 1 verdict: **accepted**

Phase 8c closes Round 1. The four commits land exactly the brief's mandate: a working `kafka-console-consumer` test-harness helper, an end-to-end byte-fidelity test that pins the CLAUDE.md §12 zero-copy guarantee at the broker boundary, the resolution of a real (not nit) rustdoc bug deferred from Phase 8b, and an accurate NOTES.md close stanza. Zero Blocking findings; one Suggestion (unnecessary Arc ceremony in the test close path); two Nits (test-helper rustdoc precision and commit-message attribution accuracy). All five integration tests pass against a live broker; lib-test count unchanged at 1233; format + lint clean.

### Next steps for Manager

- Phase 8c **closes as accept-with-followups**: Suggestion 1 and the two Nits are non-blocking and can be folded into Phase 8d (the next touch on `tests/integration/producer_smoke_test.rs` and the helper rustdoc).
- **Housekeeping (Manager-side, not Actor's job)**: the "Deferred followups carried into Phase 8c" section (`COMMENTS.8.md:553-555`) is now obsolete (the only entry has been resolved) AND it misclaims the doc-grouping fix was "Documentation polish only — no compiler/rustdoc effect". Re-verification proved it had a real rustdoc effect. Recommend either deleting the section or archiving it with a one-line "resolved in `bc0508d` — proved to have real rustdoc impact, see Phase 8c Round 1 review" note.
- **Memory note (Critic-side)**: I am updating `.claude/agent-memory/kafka-critic/` with a false-negative lesson — doc-comment grouping claims about which item rustdoc attaches to require verification via actual rustdoc HTML output (or the relevant clippy lint **with confirmation it actually fires**), not assumption-from-source-reading. The pre-fix code looked benign on the page, but the rustdoc consequence was severe.
- Phase 8d can begin; the console-consumer harness is codec-agnostic so 8d's consume side reuses the helper unchanged (the broker decompresses before serving fetches).

Phase 8c Round 1 closes. Manager advances to Phase 8d (compression matrix) plan.
