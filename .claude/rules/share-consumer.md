# Share consumer translation rules (KIP-932)

**Scope.** These rules apply to share-consumer code only:

  - files under `rust/src/consumer/` (including `internals/`,
    `internals/events/` and `internals/metrics/`) whose names match
    `*share*` or `acknowledg*`, which includes `node_acknowledgements*`. The
    `*share*` pattern covers the public API files `kafka_share_consumer.rs`,
    `mock_share_consumer.rs`, `dyn_share_consumer.rs` and
    `internals/metrics/kafka_share_consumer_metrics.rs` as well as every
    `share_*` file;
  - the share request/response wrappers under `rust/src/common/requests/`
    (`share_fetch_*`, `share_acknowledge_*`, `share_group_heartbeat_*`,
    `share_group_describe_*`, `share_request_metadata.rs`);
  - the share-specific edits to shared consumer infrastructure (new share
    fields, variants, arms and methods in `request_managers.rs`,
    `application_event*.rs`, `subscription_state.rs`, `wakeup_trigger.rs`,
    the abstract membership/heartbeat managers, and so on). Pre-existing,
    non-share code in those files is out of scope.

They supplement [consumer-threading.md](consumer-threading.md), which applies
in full to share code unless a section below says otherwise; on a conflict
inside the scope above, this file wins. Admin share-group RPCs (Milestone 18
P9) follow [admin-client.md](admin-client.md), not this file.

This milestone also pilots the rules in
[translation-checklist.md](../../design/current/Milestone-18-share-consumer/translation-checklist.md)
(R1–R8), with the same scope. That checklist lives outside `.claude/rules/` on
purpose, so it binds only share-consumer work.

The Java contract is **Apache Kafka 4.4.0** (the `kafka/` submodule at tag
`4.4.0`). Every claim about Java below cites `Class#method`; line numbers are
not used because they drift (translation-checklist R7). If Java 4.4.0
contradicts a rule here, the code wins: escalate to the Manager and do not
follow the rule.

Each numbered section is one design decision: the rule, **Why**, **How to
apply**, and **Anti-patterns** where review has something concrete to flag.

## 1. Public API shape: same as `Consumer`

The public surface mirrors the existing `Consumer` design:

  - a `ShareConsumer<K, V>` trait whose blocking methods return
    `impl Future<Output = ...> + Send`;
  - a sealed `DynShareConsumer<K, V>` companion for `Box<dyn ...>` dispatch;
  - a concrete `KafkaShareConsumer<K, V>` holding the crate-private
    `ShareConsumerImpl` delegate;
  - `MockShareConsumer<K, V>` implementing the trait.

Each concrete type is `Send + !Sync` (`PhantomData<Cell<()>>`, as
`KafkaConsumer`), and a method takes `&mut self` wherever
`ShareConsumerImpl` calls `acquireAndEnsureOpen()` or `acquire()`.
`ShareConsumerDelegate` / `ShareConsumerDelegateCreator` collapse into the
constructor, as `ConsumerDelegate` did (DoD §7 justification: Java has a
single delegate, `ShareConsumerDelegateCreator#create` only constructs
`ShareConsumerImpl` and wraps non-`KafkaException` failures as
`"Failed to construct Kafka share consumer"`; keep that message).

Only five types in `org.apache.kafka.clients.consumer` (and its subpackages)
in the closure are `@InterfaceAudience.Public`: `ShareConsumer`,
`KafkaShareConsumer`, `MockShareConsumer`, `AcknowledgeType`,
`AcknowledgementCommitCallback`. Outside that package, the three public share
errors (`InvalidShareSessionEpochException`, `ShareSessionLimitReachedException`,
`ShareSessionNotFoundException`) already exist as public Rust errors, and the
public admin share types follow `admin-client.md`. Every other consumer-package
type in the closure is crate-private, including
`ShareConsumerConfig` (no annotation), `ShareAcknowledgementMode` and
`ShareAcquireMode` (both in `consumer.internals`). Like `MockConsumer`,
`MockShareConsumer` is itself `@InterfaceAudience.Public` in 4.4, so it needs no
test-jar exception (`TEST_JAR_CLASSES` in `rust/xtask/src/java_api.rs`).

**Async vs sync per method.** A method is `async` when Java blocks in it, or
when it runs the user's (async, §3) acknowledgement callback.

| Java `ShareConsumerImpl` method | Why it blocks / what it calls | Rust |
|---|---|---|
| `subscribe(Collection)` | `applicationEventHandler.addAndGet(ShareSubscriptionChangeEvent)` | `async` |
| `unsubscribe()` | `addAndGet(ShareUnsubscribeEvent)` | `async` |
| `poll(Duration)` | waits on `ShareFetchBuffer#awaitNotEmpty`; runs the callback | `async` |
| `commitSync()` / `commitSync(Duration)` | `ConsumerUtils.getResult(commitFuture)`; runs the callback | `async` (`commit_sync`, `commit_sync_with_timeout`) |
| `commitAsync()` | does not block (`add`, not `addAndGet`) **but** calls `handleCompletedAcknowledgements()`, which runs the callback | `async`, as `Consumer::commit_async` is |
| `close()` / `close(Duration)` | `addAndGet(ShareAcknowledgeOnCloseEvent)`, `processBackgroundEvents(future, timer, ...)`, network-thread join; runs the callback | `async` (`close`, `close_with_timeout`) |
| `subscription()` | `Set.copyOf(subscriptions.subscription())` | sync |
| `acknowledge(record)`, `acknowledge(record, type)`, `acknowledge(topic, partition, offset, type)` | in-memory on `currentFetch` | sync (Result vs panic is D7) |
| `setAcknowledgementCommitCallback(callback)` | `add(ShareAcknowledgementCommitCallbackRegistrationEvent)` | sync |
| `acquisitionLockTimeoutMs()` | reads `currentFetch` | sync |
| `metrics()` | reads the registry | sync |
| `wakeup()` | `wakeupTrigger.wakeup()` | sync, `&self` |
| `clientInstanceId`, `registerMetricForSubscription`, `unregisterMetricFromSubscription` | KIP-714 | pending D2 |

Overload names follow CLAUDE.md §2. The three Java `acknowledge` overloads
(`(ConsumerRecord)`, `(ConsumerRecord, AcknowledgeType)`,
`(String, int, long, AcknowledgeType)`) have an empty parameter intersection
and Java has no zero-argument `acknowledge`, so under a strict reading of §2 no
overload gets the plain name (the precedent is `Consumer::subscribe_with_topics`,
with no plain `subscribe`). All three names are decided in P8 against §2, or by
an explicit Manager exception, and recorded in the phase PLAN. The
four-parameter form falls under §2's `Options` clause unless that exception
says otherwise.

**Why:** the `Consumer` design (`rust/src/consumer/mod.rs`, `dyn_consumer.rs`, `kafka_consumer.rs`) is settled;
diverging for the share consumer would give users two models. `commit_async`
is the one method whose classification is not obvious from "blocks in Java":
Java runs the callback inline in it (`ShareConsumerImpl#commitAsync` →
`#handleCompletedAcknowledgements`), and with an async callback (§3) the
method must await it.

**How to apply:**

  - Check every method against `ShareConsumerImpl`, not against the
    `KafkaShareConsumer` javadoc.
  - `close(Duration)` with a negative timeout returns
    `IllegalArgumentError("The timeout cannot be negative.")`; a second
    `close` is a no-op (`ShareConsumerImpl#close(Duration)` checks `closed`).
  - Java facts on the closed check. `ShareConsumerImpl#acquireAndEnsureOpen`
    throws `IllegalStateException("This consumer has already been closed.")`
    when `closed` is set, and otherwise calls
    `metadata.maybeThrowBootstrapFatalException()`. It is called by
    `#subscription`, `#subscribe`, `#unsubscribe`, `#poll`,
    `#acknowledge(ConsumerRecord, AcknowledgeType)`,
    `#acknowledge(String, int, long, AcknowledgeType)`,
    `#commitSync(Duration)`, `#commitAsync`,
    `#setAcknowledgementCommitCallback` and `#acquisitionLockTimeoutMs`
    (`#acknowledge(ConsumerRecord)` and `#commitSync()` delegate to those).
    `#close(Duration)` calls only `#acquire`. `#metrics`, `#wakeup`,
    `#clientInstanceId`, `#registerMetricForSubscription` and
    `#unregisterMetricFromSubscription` do **not** check `closed`; do not add
    a closed check to them.
  - How Rust surfaces the closed check is not decided here. For the async
    methods it is an `Err` in the existing `Result`. For the sync getters
    (`subscription`, `acquisition_lock_timeout_ms`,
    `set_acknowledgement_commit_callback`), follow the existing `Consumer`
    precedent (`Consumer::subscription` / `Consumer::paused` in
    `rust/src/consumer/mod.rs` return plain values and document a panic on
    `KafkaConsumer`). For `acknowledge*`, the shape is decision D7.

**Anti-patterns:**

  - A sync `commit_async` that cannot await the callback, or that defers the
    callback to a spawned task.
  - `async fn` on `acknowledge*`, `subscription`, `set_acknowledgement_commit_callback`.
  - A `block_on` facade (consumer-threading.md §1).

## 2. One background task per instance, same code as the consumer

`ShareConsumerImpl` builds its background thread from the same classes as
`AsyncKafkaConsumer`: `ApplicationEventHandler` (which owns
`ConsumerNetworkThread`), `ApplicationEventProcessor`, `RequestManagers`,
`NetworkClientDelegate`, `BackgroundEventHandler` and `CompletableEventReaper`
(`ShareConsumerImpl` constructor; `RequestManagers#supplier` share overload).
The Rust translation does the same: one `tokio::spawn`ed background task per
share-consumer instance, running the existing consumer task code. There is no
second network-thread implementation.

  - `RequestManagers` gets share `Option` fields. In share mode its entry
    order is Java's: coordinator, share heartbeat, share membership, share
    consume (`RequestManagers` share constructor). The consumer-only managers
    are absent (`Optional.empty()` or `null` in Java).
  - `ApplicationEvent` gets the share variants of §5, and
    `ApplicationEventProcessor` gets the share arms
    (`ApplicationEventProcessor#process` switch).
  - consumer-threading.md §10 applies verbatim: the network poll is never
    raced in a `select!`; wake it through the selector wakeup.

**Why:** Java shares the code, and the share managers plug into it exactly as
the consumer managers do. A parallel task implementation would duplicate the
most bug-prone code in the crate.

**How to apply:** keep share edits to shared files additive (new fields,
variants, arms). Any reshaping of shared infrastructure goes in its own
behaviour-neutral commit that the Critic can review without share semantics.
See `infra-audit.md` in the milestone directory for which classes need that.

**Anti-patterns:** a `ShareConsumerNetworkThread` type; a second
`tokio::spawn` per instance for share fetch or acknowledgements; per-request
spawns on the share path (CLAUDE.md §13).

## 3. Callback call sites and thread

`AcknowledgementCommitCallback` is an `#[async_trait]` trait, like
`OffsetCommitCallback` (decision D8, settled 2026-11-18):

    #[async_trait]
    pub trait AcknowledgementCommitCallback: Send + Sync + 'static {
        async fn on_complete(
            &self,
            offsets: &HashMap<TopicIdPartition, HashSet<i64>>,
            error: Option<&Error>,
        );
    }

and `commit_sync` returns `HashMap<TopicIdPartition, Option<Error>>`
(Java `Map<TopicIdPartition, Optional<KafkaException>>`).

The callback runs on the **caller's task**, inside
`handle_completed_acknowledgements`, and that method is called exactly where
`ShareConsumerImpl#handleCompletedAcknowledgements` is called in Java 4.4.0:

| Caller | Where in the method |
|---|---|
| `ShareConsumerImpl#poll` | after `processBackgroundEvents()` at the start; again when a non-empty fetch is returned; again before returning empty after the timer expires |
| `ShareConsumerImpl#commitSync(Duration)` | at the start; again after the sync result is collected (success path only) |
| `ShareConsumerImpl#commitAsync` | at the start |
| `ShareConsumerImpl#close(Duration, boolean)` | once, after `sendAcknowledgementsAndLeaveGroup` and `stopFindCoordinatorOnClose`, inside `swallow(...)` |

`processBackgroundEvents()` (background **error** events; the share consumer
has no rebalance-listener events) is called from:

| Caller | How |
|---|---|
| `ShareConsumerImpl#poll` | at the start, and once per loop iteration when the fetch was empty |
| `ShareConsumerImpl#close` | via `#sendAcknowledgementsAndLeaveGroup` → `processBackgroundEvents(unsubscribeEvent.future(), timer, ignore)`, then `#processBackgroundEventsOnClose` |

`unsubscribe`, `subscribe` and `commitSync` do **not** call
`processBackgroundEvents()`; they call `addAndGet` / `getResult` directly.

Callback semantics, from `AcknowledgementCommitCallbackHandler#onComplete`:

  - It is invoked **once per `(TopicIdPartition, Acknowledgements)` entry** of
    each completed acknowledgements map, with a single-entry offsets map
    `{partition: offsets}` and that entry's `getAcknowledgeException()`.
    It is not invoked once per request or once per map.
  - An exception thrown by the callback is logged
    (`"Exception thrown by acknowledgement commit callback"`) and swallowed;
    the remaining entries still get their callback. The settled D8 signature
    has no `Result`, which matches this; if a `Result` is ever added for
    symmetry with `OffsetCommitCallback`, an `Err` must be logged and
    swallowed, never propagated (this differs from `OffsetCommitCallback`,
    whose errors propagate).
  - Completed acknowledgements reach the app side as `ShareAcknowledgementEvent`s
    on their own queue (§5). `ShareConsumerImpl#processAcknowledgementEvents`
    drains it at the top of `handleCompletedAcknowledgements`. The processor
    (`ShareConsumerImpl.ShareAcknowledgementEventProcessor#process`) appends to
    `completedAcknowledgements` **iff a callback handler is registered**, and
    calls `currentFetch.renew(...)` **iff the event's
    `checkForRenewAcknowledgements` is set**; the two conditions are
    independent.
  - The background side emits the event iff
    `isAcknowledgementCommitCallbackRegistered || checkForRenewAcknowledgements`
    (`ShareConsumeRequestManager#maybeSendShareAcknowledgementEvent`). The
    second operand is a caller-supplied flag, **not** "the map contains a
    RENEW": the timeout, failure, missing-node, no-leader and invalid-response
    paths pass a literal `true` (e.g.
    `ShareConsumeRequestManager.AcknowledgeRequestState#handleAcknowledgeTimedOut`,
    `#failPendingAcknowledgementsCannotBeSent` and
    `#processPendingInFlightAcknowledgements`; and
    `ShareConsumeRequestManager#maybeAddAcknowledgements`,
    `#removeSessionHandlersForMissingNodes` and the no-leader paths of
    `#commitAsync` / `#acknowledgeOnClose`), while the fetch-piggybacked
    response paths (`#handleShareFetchSuccess` / `#handleShareFetchFailure`)
    pass the request's `isRenewAck()`. Re-derive the full caller list in P5.
  - `setAcknowledgementCommitCallback(null)` clears
    `completedAcknowledgements`; registration changes are sent to the
    background task with `ShareAcknowledgementCommitCallbackRegistrationEvent`
    only on a null↔non-null transition.
  - While the callback runs, Java rejects every consumer call with
    `IllegalStateException("KafkaShareConsumer methods are not accessible from user-defined acknowledgement commit callback.")`
    (`ShareConsumerImpl#acquire`). In Rust the consumer is mutably borrowed
    during the call and the callback receives no consumer reference, so this
    is a compile-time guarantee; how `wakeup` is reachable is decision D6.

**Why:** lesson VC-01 — consumer-threading.md §31 once listed call sites from
the Rust design rather than from Java, and the code followed the rule. The
`AcknowledgementCommitCallback` javadoc says "any thread calling `poll`", but
the code also calls it from `commitSync`, `commitAsync` and `close`; the code
is the contract. CLAUDE.md §11.5 requires the same exactly-once obligation at
the same lifecycle points.

**How to apply:**

  - Call `handle_completed_acknowledgements` at exactly the sites above, in
    the same order relative to `acknowledgeBatchIfImplicitAcknowledgement`,
    `ensureInFlightAcknowledgedIfExplicitAcknowledgement` and the event
    submissions.
  - Await the callback inline on the caller's task. Do not spawn it.
  - Drain `completedAcknowledgements` even if the callback fails
    (`finally { completedAcknowledgements.clear(); }`).

**Anti-patterns:**

  - `tokio::spawn(callback.on_complete(...))`, or invoking the callback from
    the background task.
  - A `handle_completed_acknowledgements` call in `unsubscribe` or
    `subscribe`, or a missing one in `commit_async`.
  - A `process_background_events` call in `unsubscribe` (Java has none).
  - One callback per request or per map instead of per partition entry.
  - Propagating an error from the callback out of `poll` / `commit_*` / `close`.

## 4. Receive path

`ShareCompletedFetch` follows consumer-threading.md §27: one owned response
buffer, every record and header borrows from it, no per-record topic `String`
(clone the per-partition `Arc<str>`), the `Deserializer<T>` trait takes
`&[u8]`. It reuses the consumer's record-batch decoder in whatever form it has
when P3a starts; it does not fork it. Share fetch is a per-record hot path
(CLAUDE.md §13): P3a adds a per-record allocation-budget test following the
consumer precedent.

How `ShareInFlightBatch` keeps records for redelivery after `RENEW`
(`ShareInFlightBatch#takeRenewals`, `ShareFetch#takeRenewedRecords`) is
**decision D5, not a rule**. The buffer-ownership facts the decision needs are
in the milestone's `infra-audit.md`.

**Anti-patterns:** a pre-decoded `Vec<ConsumerRecord>` in `ShareCompletedFetch`
(§27); a second copy of the batch decoder; any record-retention shape for
RENEW introduced before D5 is decided.

## 5. Event table

Classified from each Java class's `extends`, per consumer-threading.md §28.
A completable event carries a `CompletableEventHandle<T>`; a bare one does not.

| Java class | `extends` | Kind | Rust payload | Processor arm (`ApplicationEventProcessor#process(...)`) |
|---|---|---|---|---|
| `SharePollEvent` | `ApplicationEvent` | bare, with its own completion flag (`volatile boolean isComplete`, `completeSuccessfully()`), like `AsyncPollEvent` | `deadline_ms: i64`, `poll_time_ms: i64`, shared `is_complete` (an atomic the app side can read) | `shareMembershipManager.maybeReconcile(true)`; `membershipManager.onConsumerPoll()`; `hrm.resetPollTimer(pollTimeMs)`; `completeSuccessfully()` |
| `ShareFetchEvent` | `ApplicationEvent` | bare | `acknowledgements_map: HashMap<TopicIdPartition, NodeAcknowledgements>` | `shareConsumeRequestManager.fetch(map)` if present |
| `ShareAcknowledgeAsyncEvent` | `ApplicationEvent` | bare | `acknowledgements_map`, `deadline_ms` | `commitAsync(map, deadlineMs)` if present |
| `ShareAcknowledgementCommitCallbackRegistrationEvent` | `ApplicationEvent` | bare | `is_callback_registered: bool` | `setAcknowledgementCommitCallbackRegistered(b)` if present |
| `ShareAcknowledgeSyncEvent` | `CompletableApplicationEvent<Map<TopicIdPartition, Acknowledgements>>` | completable | `acknowledgements_map`, deadline; handle `T = HashMap<TopicIdPartition, Acknowledgements>` | `commitSync(map, deadlineMs).whenComplete(complete(future))`; **returns without completing the future** if the manager is absent (the reaper expires it) |
| `ShareAcknowledgeOnCloseEvent` | `CompletableApplicationEvent<Void>` | completable, `T = ()` | `acknowledgements_map`, deadline | `acknowledgeOnClose(map, deadlineMs)`; if absent, fails with `KafkaException("Group membership manager not present when processing an acknowledge-on-close event")` |
| `ShareSubscriptionChangeEvent` | `CompletableApplicationEvent<Void>` | completable, `T = ()`, deadline `Long.MAX_VALUE` | `topics: HashSet<String>` | `subscriptions.subscribeToShareGroup(topics)` (→ `metadata.requestUpdateForNewTopics()` if changed), `membershipManager().onSubscriptionUpdated()`, complete; if absent, fails with `"Group membership manager not present when processing a subscribe event"` |
| `ShareUnsubscribeEvent` | `CompletableApplicationEvent<Void>` | completable, `T = ()` | deadline | `subscriptions.unsubscribe()`, `membershipManager().leaveGroup().whenComplete(complete(future))`; if absent, fails with `"Group membership manager not present when processing an unsubscribe event"` |
| `ShareAcknowledgementEvent` | nothing (plain class; **not** a `BackgroundEvent`) | bg → app message on its own queue (`ShareAcknowledgementEventHandler`, a `LinkedBlockingQueue`) | `acknowledgements_map: HashMap<TopicIdPartition, Acknowledgements>`, `check_for_renew_acknowledgements: bool`, `acquisition_lock_timeout_ms: Option<i32>` | none: drained by `ShareConsumerImpl#processAcknowledgementEvents` on the app side |

`ShareAcknowledgementEventHandler` (`add`, `drainEvents`) is the queue wrapper
for the last row. The share close path also submits the shared
`StopFindCoordinatorOnCloseEvent` (`ShareConsumerImpl#stopFindCoordinatorOnClose`);
no share variant is needed for it. There are 8 `Share*Event` application
events, 1 `ShareAcknowledgementEvent` and 1 handler: 10 files under
`internals/events/`.

**How to apply:** do not add fictional fields, do not drop `deadline_ms`
or `poll_time_ms`, keep the `KafkaException` texts verbatim, and keep the
"silently return" behaviour of the sync-acknowledge arm (it is Java's).

**Anti-patterns:** routing `ShareAcknowledgementEvent` through the
`BackgroundEvent` channel (it has its own queue and its own drain point, and
processing errors there are logged and swallowed, not thrown —
`ShareConsumerImpl#processAcknowledgementEvents`); a completable
`SharePollEvent`; `CompletableEventHandle<()>` for `ShareAcknowledgeSyncEvent`.

## 6. Acknowledgement-state ownership

| State (Java) | Owner | Rust |
|---|---|---|
| `ShareConsumerImpl#currentFetch` (`ShareFetch`: per-partition `ShareInFlightBatch` lists, each with its in-flight offsets and `Acknowledgements`) | app task only | plain field on `ShareConsumerImpl`, no lock |
| `completedAcknowledgements`, `acknowledgementCommitCallbackHandler`, `inFlightPoll`, `shouldSendShareFetchEvent` | app task only | plain fields |
| `ShareFetchBuffer` (completed fetches; `ReentrantLock` + `Condition`) | shared: bg adds (`ShareConsumeRequestManager`), app drains (`ShareFetchCollector`), `WakeupTrigger` wakes | `std::sync::Mutex` + `tokio::sync::Notify`, as the consumer's `FetchBuffer` |
| `SharePollEvent#isComplete` (`volatile`) | written bg, read app | atomic |
| acknowledgement-event queue (`LinkedBlockingQueue<ShareAcknowledgementEvent>`) | bg → app | channel, drained with `try_recv` in a loop |
| `SubscriptionState` | shared | consumer-threading.md §16 (`Arc<std::sync::Mutex<_>>`) |
| `ShareConsumeRequestManager` maps (`fetchAcknowledgementsToSend`, `fetchAcknowledgementsInFlight`, `acknowledgeRequestStates`, `sessionHandlers`, `nodesWithPendingRequests`), `isAcknowledgementCommitCallbackRegistered`, `closing`, `fetchRecordsNodeId` | bg task only | plain fields on the manager. `fetchRecordsNodeId` is an `AtomicInteger` in Java, but all its uses are inside the manager's request-building and response-handling paths, which run on the network thread; P5a confirms this before choosing a plain `i32` |

Acknowledgements change owner by moving through events:
`ShareFetch#takeAcknowledgedRecords` hands `NodeAcknowledgements` to a
`ShareFetchEvent` / `ShareAcknowledge*Event` (app → bg), and completed
`Acknowledgements` come back in a `ShareAcknowledgementEvent` (bg → app).
In `ShareConsumeRequestManager.ResultHandler#complete` the same
`Acknowledgements` object goes both into the event and into the
`commitSync` future's result; both readers only read it afterwards. In Rust,
move by value through the channel; where Java has two readers, a clone or an
`Arc` is acceptable. Acknowledgements handed **app → bg** are owned and
mutated by the background task afterwards (merged in
`ShareConsumeRequestManager#commitSync` / `#commitAsync` / `#acknowledgeOnClose`
/ `#processAcknowledgementsMap`, completed via `Acknowledgements#complete` on
response, timeout and failure). Once placed in a `ShareAcknowledgementEvent`
or a `commitSync` result (**bg → app**), an `Acknowledgements` is not
mutated.

**Locks:** `std::sync::Mutex` only, never `tokio::sync::Mutex`, never held
across `.await` (CLAUDE.md §11.6), never held while the user callback runs.
Lock order where two are needed: decide in the phase that first takes both,
and state it at the struct definition.

**Anti-patterns:** `Arc<Mutex<ShareFetch>>` (it is app-confined);
the acknowledgement maps behind a lock shared with the app side; a guard
alive across the callback await or the fetch-buffer wait.

## 7. Acknowledgement-mode semantics

`share.acknowledgement.mode` is `implicit` (default) or `explicit`
(`ShareAcknowledgementMode#fromString`; **case-sensitive**, lower-case only: `"IMPLICIT"` is rejected, `ShareAcknowledgementModeTest`). `share.acquire.mode` is `batch_optimized` (default) or `record_limit` and, unlike the acknowledgement mode, is parsed case-insensitively (`ShareAcquireMode#of` upper-cases the name). The exact Java texts
below are part of the contract and tests assert them verbatim.

| Situation | Java site | Error |
|---|---|---|
| `acknowledge*` called in implicit mode | `ShareConsumerImpl#ensureExplicitAcknowledgement` | `IllegalStateException("Implicit acknowledgement of delivery is being used.")` |
| `acknowledge(record[, type])` for a record not in flight | `ShareFetch#acknowledge(ConsumerRecord, AcknowledgeType)`, `ShareInFlightBatch#acknowledge` | `IllegalStateException("The record cannot be acknowledged.")` |
| `acknowledge(topic, partition, offset, type)` for an offset that is not in a batch whose delivery failed | `ShareFetch#acknowledge(String, int, long, AcknowledgeType)` | `IllegalStateException("The record cannot be acknowledged.")` |
| `poll` in explicit mode with unacknowledged in-flight records | `ShareConsumerImpl#ensureInFlightAcknowledgedIfExplicitAcknowledgement` | `IllegalStateException("All records must be acknowledged in explicit acknowledgement mode.")` |
| `poll` with no subscription | `ShareConsumerImpl#poll` | `IllegalStateException("Consumer is not subscribed to any topics.")` |
| renew while acknowledgements are incomplete | `ShareInFlightBatch#renew(Acknowledgements)` | `IllegalStateException("Renewing with uncompleted acknowledgements")` |
| invalid mode string | `ShareAcknowledgementMode#fromString`; `ShareAcknowledgementMode.Validator#ensureValid` | `"Acknowledgement mode is null"` / `"Invalid acknowledgement mode: <v>"`; validator: ``ConfigException(name, value, "Invalid value `<v>` for configuration <name>. The value must either be 'implicit' or 'explicit'.")`` |
| unsupported config key set | `ShareConsumerConfig#checkUnsupportedConfigsPreProcess` | `ConfigException("<keys joined by \", \"> cannot be set when using a share group.")` for any of `auto.offset.reset`, `enable.auto.commit`, `group.instance.id`, `isolation.level`, `partition.assignment.strategy`, `interceptor.classes`, `session.timeout.ms`, `heartbeat.interval.ms`, `group.protocol`, `group.remote.assignor` |

Behaviour:

  - **Implicit:** at the start of `poll`, `commitSync` and `commitAsync`,
    `ShareConsumerImpl#acknowledgeBatchIfImplicitAcknowledgement` acknowledges
    every in-flight record of the current batch as `ACCEPT`
    (`ShareFetch#acknowledgeAll`, which does not overwrite existing
    acknowledgements — `ShareInFlightBatch#acknowledgeAll` uses `addIfAbsent`).
  - **`close` does not acknowledge implicitly**, in either mode.
    `ShareConsumerImpl#close(Duration, boolean)` →
    `#sendAcknowledgementsAndLeaveGroup` sends only
    `acknowledgementsToSend()` (`currentFetch.takeAcknowledgedRecords()`) and
    never calls `#acknowledgeBatchIfImplicitAcknowledgement`; unacknowledged
    acquired records are released by the broker (`KafkaShareConsumer` class
    javadoc: "Calling `close()` which releases any acquired records without
    acknowledgement."). Do not add an `acknowledge_all(ACCEPT)` to `close`.
  - **Explicit:** `poll` first checks that every in-flight record has been
    acknowledged and fails otherwise; `commitSync` / `commitAsync` do not
    check.
  - `acknowledge(record)` is `acknowledge(record, ACCEPT)`
    (`ShareConsumerImpl#acknowledge(ConsumerRecord)`).
  - Acknowledging with `RENEW` marks the batch for renewal
    (`ShareInFlightBatch#addAcknowledgement` / `#acknowledge` set
    `checkForRenewAcknowledgements`).
  - `AcknowledgeType` ids are `ACCEPT=1, RELEASE=2, REJECT=3, RENEW=4`; gap
    records use `Acknowledgements.ACKNOWLEDGE_TYPE_GAP = 0`.
  - Whether the misuse cases return `Result` or panic is decision D7; the
    message text is fixed either way.

## 8. Wakeup

Java observes `wakeup()` only at these points:
`ShareConsumerImpl#poll` (`wakeupTrigger.maybeTriggerWakeup()` at the top of
each loop iteration, and `setShareFetchAction(fetchBuffer)` around
`ShareFetchBuffer#awaitNotEmpty`, whose `wakeup` ends the wait), and
`ShareConsumerImpl#commitSync(Duration)` (`setActiveTask(commitFuture)`).
`close` calls `disableWakeups()` first. `subscribe` / `unsubscribe` do not
register a wakeup task. The existing Rust `WakeupTrigger` cancellation token
(consumer-threading.md §11) already covers what Java's `ShareFetchAction` does,
so no new action type is added (see `infra-audit.md`); the share poll's
fetch-buffer wait must end on the token exactly as the consumer's poll does.
How a second thread reaches
`wakeup(&self)` while `poll` holds `&mut self` is decision D6.

**Anti-patterns:** waking the background task through `WakeupTrigger`
(consumer-threading.md §31: use `ApplicationEventHandler::wake_background_task`,
the analog of Java's `applicationEventHandler.wakeupNetworkThread()` that
`ShareConsumerImpl#collect` and `#sendShareAcknowledgeAsyncEvent` call).

## 9. Critic checklist

For every share-consumer commit, the Critic checks at least:

  1. **Exactly-once callback.** Each completed `(partition, Acknowledgements)`
     entry produces exactly one `on_complete` when a callback is registered,
     and none when it is not. No path drops a `ShareAcknowledgementEvent`, and
     none emits two for one acknowledge request (look at retries, leader
     change and close).
  2. **Call sites.** `handle_completed_acknowledgements` and
     `process_background_events` are called at exactly the §3 sites, in
     Java's order, re-derived from `ShareConsumerImpl` rather than from this
     file.
  3. **Acknowledgement ordering.** Implicit acknowledgement happens before
     `take_acknowledged_records`; acknowledgements piggy-back on the next
     `ShareFetchEvent` or go in a `ShareAcknowledgeAsyncEvent` exactly as
     `ShareConsumerImpl#collect` decides; only one `ShareFetchEvent` per
     `poll` (`shouldSendShareFetchEvent`).
  4. **Leader change re-routing.** When a partition's leader changes,
     pending acknowledgements are failed or re-routed as
     `ShareConsumeRequestManager` does (`shareSessionLeaderMap` handling and
     the `NotLeaderOrFollower` paths), not silently dropped or sent to the
     old leader.
  5. **No double acknowledgement, no lost acknowledgement.** No record is
     acknowledged twice (implicit `acknowledgeAll` uses add-if-absent), and
     every acknowledgement recorded before close is sent in
     `ShareAcknowledgeOnCloseEvent`. Close does **not** acknowledge
     unacknowledged records; the broker releases them (§7).
  6. **Events** match §5: completable vs bare, payload fields, error texts.
  7. **Ownership** matches §6: no lock across `.await` or across the
     callback; app-confined state not behind a shared lock.
  8. **Error texts** in §1 and §7 asserted verbatim in tests.
  9. **Pilot rules R1–R8** (translation-checklist.md). Tag each comment that
     relies on one with its rule number, so the canary can count them.
  10. **Pending decisions** (below): flag code that silently commits to an
      undecided design; do not flag code for not following an undecided one.

## Pending decisions

These are open in `design/current/Milestone-18-share-consumer/PLAN.md` §7 and
are **not rules**. Do not translate around them and do not cite them as
rules in review:

  - **D2** — KIP-714 methods (`clientInstanceId`, `registerMetricForSubscription`,
    `unregisterMetricFromSubscription`) on `ShareConsumer`.
  - **D5** — record retention for `RENEW` (KIP-1222). Gates P3a. No rule.
  - **D6** — cross-thread `wakeup` (a `ShareConsumerHandle` or another shape).
  - **D7** — `acknowledge*` misuse: `Result` or panic.

Settled on 2026-11-18 and recorded in PLAN §7: D1 (Admin share-group RPCs in
scope, P9), D3 (four PRs), D8 (§3 callback shape).
