# Multilanguage conversion: findings

The Rust-only broker integration tests in `rust/tests/integration/` are being
converted into multilanguage scenarios (`multilanguage_test!`,
`multilanguage_consumer_test!`, `multilanguage_admin_test!`), so each one also
runs against the Python (sync and asyncio) and C clients through the gRPC
backends. A new arm can show that a binding behaves differently from the Rust
client and from Java. This file lists every such difference. The bindings are
not changed by the conversion; a finding is recorded here and handled
separately.

A test-only cause (a timing margin, a wrong fixture) is fixed in the test and
is not listed.

Each entry gives the scenario, the arms that show the difference, what Java and
the Rust client do, what the arm does, the failing assertion's text, and the
state:

- **open**: the scenario still runs on every arm and fails on the arms named.
- **reverted to Rust-only**: the test went back to its Rust-only form, marked
  `// MULTILANGUAGE: blocked by finding #k`, so the suite stays green.
- **needs owner**: the conversion cannot continue without a decision.
- **fixed on this branch (separate PR to follow)**: the cause is fixed by commits on this branch
  that can be taken out into their own PR, and the scenario runs on every arm again.
- **skipped in the body on the affected arms**: the scenario stays multilanguage; the arms named
  return at the start of the body with a `[multilanguage] <test> backend=<arm> skipped: blocked by
  finding #k` stderr line, marked `// MULTILANGUAGE: blocked by finding #k`, and the other arms run.

## Findings

### 1. Python: a listener released by `subscribe([])` is still called by `close()`

- **Scenario**: `plaintext_consumer_subscription_test::test_async_consumer_unsubscribe_topic`
  (Java `PlaintextConsumerSubscriptionTest#testUnsubscribeTopic`): `subscribe([topic], listener)`,
  wait for the `assigned` callback, `subscribe([])`, assert the assignment is empty, `close()`.
- **Arms**: `__grpc_python` and `__grpc_python_async` fail; `__rust` and `__grpc_c` pass.
- **Java / Rust**: `subscribe(emptyList)` is `unsubscribe()`
  (`AsyncKafkaConsumer#subscribeInternal`), which keeps the subscription's rebalance listener.
  `close()` then runs `AsyncKafkaConsumer#runRebalanceCallbacksOnClose` on that same listener: the
  native arm and the C arm log `assigned`, `revoked` and then `lost` (2 partitions), and `close()`
  returns `Ok`.
- **Why `close()` reports 2 partitions when `assignment()` is empty**: this is Java 4.4's own
  behaviour, which the core mirrors (`clear_assignment_and_leave_group`,
  `run_rebalance_callbacks_on_close`); it is not a core bug. Java's
  `AbstractMembershipManager#clearAssignmentAndLeaveGroup` calls `SubscriptionState#unsubscribe`
  before `AbstractMembershipManager#clearAssignment`, whose `notifyAssignmentChange(emptySet())`
  runs only while `hasAutoAssignedPartitions()`, which is false after the unsubscribe. So the group
  assignment snapshot keeps the partitions, the reset group metadata has epoch -1, and
  `runRebalanceCallbacksOnClose` calls `onPartitionsLost` with them.
- **Cause**: `python/_confluentkafka.c`, `consumer_listener_commit`. After any successful
  `subscribe*` it frees the previous `ListenerCtx` (`Py_DECREF` of the binding's
  `_ListenerAdapter`, then `PyMem_Free`) and keeps the new one (NULL for a listener-less subscribe),
  on the assumption that the core no longer references the old one. That holds for a non-empty
  topic list, where the core replaces the listener. It is false for `subscribe([])`: the core runs
  `unsubscribe()` and keeps the listener. `subscribe(topics=[], callback=X)` is affected the same
  way: the binding keeps X's context, which the core ignores, and frees the old one, which the
  core still calls. The test servers are not involved: both build the listener per `Subscribe`
  RPC and keep no reference to it, as a Java caller may; the binding's context is its only owner.
  - Python sync: the freed context is used in `close()`; the server process dies with `SIGSEGV`
    (container exit 139), and the test sees
    `python gRPC backend transport error (Unknown): transport error`.
  - Python asyncio: the core logs `User provided listener failed on invocation of
    onPartitionsLost ...: LocalCallbackError: 'function' object has no attribute '_on_lost'`, and
    the test fails with
    `python_async backend: consumer close should succeed: Failed to close kafka consumer`.
    What the core calls is the freed context, not only the freed adapter: `PyMem_Free` (pymalloc,
    CPython 3.11 in the image) writes its free-list link into the block's first word,
    `ctx->adapter`. Inference, not traced: the block (16 bytes, the size class of a `VoidCbCtx`
    too) is reused by `py_Consumer_close_cb`'s `VoidCbCtx`, whose one field is `close()`'s own
    completion `cb`, a Python function, so `ctx->adapter` reads that function; the second word,
    `ctx->consumer`, is untouched, so the error is reported cleanly.
- **Texts that state the wrong release point** (each checked verbatim):
  - `kafka_consumer_Consumer_subscribe_with_topics_listener`, the registering function: "its `self`
    must stay alive until the next `subscribe_*`, `unsubscribe` or the consumer's destruction
    releases it". `kafka_consumer_Consumer_subscribe_with_pattern_listener` points to it ("see
    `subscribe_with_topics_listener` for the listener's lifetime") and both `_cb` twins say "As
    the blocking form". It is also the C form of the `subscribe(topics=[], callback=X)` case: the
    core ignores X and keeps the old listener, whose `self` this text lets the caller free.
  - `kafka_consumer_ConsumerRebalanceListener_new` (`# Safety`: the caller keeps `self_` alive
    "until the next `subscribe_*`, `unsubscribe` or the consumer's destruction releases it"),
    `kafka_consumer_Consumer_subscribe_with_topics` ("Replaces the listener registered by an
    earlier `subscribe_*`, releasing its `self`") and `kafka_consumer_Consumer_unsubscribe`
    ("releases the registered listener").
  - The Python `Consumer.subscribe` docstring (`python/confluent_kafka/consumer/consumer.py`):
    "Subscribing without one releases the previous listener." For `topics=[]` this is the
    defect, and it contradicts the same docstring's "An empty ``topics`` is the same as
    ``unsubscribe()``". A binding fix must change it.
  - Smaller texts resting on the same release point: the SAFETY comment of
    `ListenerRegistration::fire` ("which another operation cannot overlap to release the
    listener", after the same "until the next `subscribe_*`, `unsubscribe` or the consumer's
    destruction" wording); the core comment in `AsyncKafkaConsumer::unsubscribe` ("The retained
    listener is released by the next `subscribe(...)`", which misses `subscribe([])`); and both
    test servers' comments (`python/grpc_server.py` `Subscribe`: "a listener-less Subscribe
    releases it"; `c/grpc_server/server.cc` at its `subscribe_with_topics_listener` call: "the next
    subscribe_*, unsubscribe, or the consumer's destruction").

  The core keeps the listener after both `unsubscribe()` and `subscribe_with_topics([])`, and
  `close()` calls it whenever the group-assignment snapshot is non-empty, so a C caller who frees
  `self_` where the header allows gets the same use-after-free. The C arm passes only because the
  C server's `self_` (its `LogState`) lives for the whole session and so outlives every release
  point; the arm never tests the release point. Fixing the binding alone would leave the C
  contract wrong.
- **Target release point** (what the core does, and so Java's `SubscriptionState`, which only the
  `subscribe(...)` overloads write): the listener, and the `self` behind it, stays alive until a
  later successful `subscribe_*` with a non-empty topic list or with a pattern replaces it (with a
  new listener or with none), or until the consumer is closed or destroyed (the core drops the
  listener at the end of `close()`, after its last callback). `unsubscribe()`, `subscribe([])`
  (with or without a listener) and `assign(...)` keep it, and `close()` may still call it. The
  binding's `ListenerCtx` comment already says this for `unsubscribe()` ("an unsubscribe leaves
  the Java listener registered"); the binding, the C header texts and the comments above should
  all say it in one text.
- **State**: fixed on this branch, in the `finding-1: ` commits and their fixups; a separate PR is
  to follow (owner ruling 1). The binding keeps the earlier `ListenerCtx` after a `KafkaConsumer`'s
  `subscribe([])` and drops the new one, and parks the context of a failed subscribe until the
  consumer is destroyed (the core may hold it, see finding #2). The C header texts, the `fire`
  SAFETY comment and the Python `subscribe` docstring state the target release point, with two
  differences for a `MockConsumer`, which follows Java's mock: its `subscribe([])` replaces the
  listener (`SubscriptionState#subscribe` registers it whatever the topics), and its `close()` does
  not release it (`rebalance` and `losePartitions` still call it).
  - Tests: `python/test/integration/test_listener_release_broker.py` (the broker cases, and a
    broker-free FFI-mock case for the failed subscribe; both fail on the binding before the fix)
    and `test_empty_subscribe_keeps_the_listener_until_destroy` in
    `c/tests/test_consumer_callbacks.c`, which shows the C sequence but cannot catch the defect:
    without a broker no listener method runs, and the defect was in the Python binding.
  - The test servers' comments are corrected in a harness commit.
  - Left for the owner: the core comment in `AsyncKafkaConsumer::unsubscribe` ("The retained
    listener is released by the next `subscribe(...)` ... or when the consumer is dropped") misses
    `subscribe([])`, which keeps it, and `close()`, which releases it, and a failed `subscribe(...)`,
    which leaves the app-side mirror as it is (finding #2). It is shipped code outside ruling 1's
    files, so it stays as it is.
  - The scenario is back on every backend (Actor 121's un-revert). Found by the Java-parity change
    of COMMENTS.121 item 5 (owner decision 4, approved); the previous form of the test registered
    no listener and so never reached it.

### 2. Rust core: after a failed `subscribe`, a `KafkaConsumer` calls the listener from before the call, not the one the call left registered

- **Scenario** (described; no converted test exercises it): a `subscribe` that fails after the
  background thread processed its `TopicSubscriptionChangeEvent` (or
  `TopicRe2JPatternSubscriptionChangeEvent`), while the consumer stays in the group. The clean
  reproduction:
  1. `subscribe(pattern, L1)`, then poll until the consumer is assigned;
  2. `subscribe([t2], L2)` (or `subscribe([t2])`): it fails by itself with the
     mixed-subscription-type `IllegalStateException`, since a pattern and topics are two
     subscription types, and the consumer keeps its pattern subscription and its membership;
  3. a reassignment (a second member joins, or a new topic matches the pattern), polled.

  The reverse order fails the same way: after `subscribe([t1], L1)`, `subscribe(pattern, L2)` (or
  `subscribe(pattern)`) fails the type check by itself and the consumer keeps `[t1]` and its
  membership (`SubscriptionState#subscribe(SubscriptionPattern,Optional)` and the core's
  `SubscriptionState::subscribe_with_pattern`, which `process_topic_re2j_pattern_subscription_change`
  calls, also register first, and `subscribe_to_regex` writes the mirror only after `Ok`). The table holds with that call as the failed call. A `subscribe` whose event times
  out while the background thread still processes it lands the same way. Only an `assign` in
  between needs an `unsubscribe()` first, which leaves the group; even then `close()` runs Java
  4.4's stale-snapshot `onPartitionsLost` (finding #1), Java on the slot, the core on the mirror,
  so on the wrong listener too.
- **Arms**: every arm, since the core decides it (`__rust`, and the bindings over it).
- **Java 4.4**: one slot. `ApplicationEventProcessor#process(TopicSubscriptionChangeEvent)` calls
  `SubscriptionState#subscribe(Set,Optional)`, which runs `registerRebalanceListener(listener)`
  before `setSubscriptionType` throws, so after the failure the slot holds what the failed call
  carried: L2, or nothing for a listener-less call. Every invocation reads that slot:
  `ConsumerRebalanceListenerInvoker` (`subscriptions.rebalanceListener()`), `AsyncKafkaConsumer`'s
  `process(PartitionsAssignedEvent)` (`subscriptions.rebalanceListener().isEmpty()`), and
  `runRebalanceCallbacksOnClose` through the invoker. `ConsumerNetworkThread` processes an event
  whatever its deadline, so a timed-out subscribe lands too.
- **Rust**: two slots, read for different things.
  - The background slot: `process_topic_subscription_change` runs the same
    `SubscriptionState::subscribe_with_topics` (register first, then the type check), so it holds
    what the failed call carried. It is read only for presence, and only for revoke, lost and the
    unsubscribe leave
    (`AbstractMembershipManager::enqueue_rebalance_callback_with_ack_through_application_events`
    queues those events only when it holds a listener). `PartitionsAssigned` is queued whatever it
    holds (`enqueue_partitions_assigned_event`).
  - The app-side mirror `AsyncKafkaConsumer::rebalance_listener`: every invocation reads it (the
    `PartitionsRemoved` and `PartitionsAssigned` handling in `process_background_events`, and
    `run_rebalance_callbacks_on_close`). `subscribe_internal_topics` and `subscribe_to_regex` write
    it only after the event completed `Ok`, so after the failure it still holds the earlier
    listener.
- **What each one calls afterwards**:

  | Listener before | The failed call | Java | Rust |
  |---|---|---|---|
  | L1 | `subscribe([t2], L2)` | L2 gets every callback | L1 gets every callback |
  | L1 | `subscribe([t2])` | no listener runs | L1 gets `on_partitions_assigned` on every assignment, and the close callback, but never a revoke, a lost or the unsubscribe leave (the background slot is empty) |
  | none | `subscribe([t2], L2)` | L2 gets every callback | no listener runs (the revoke and lost events are queued, the mirror is `None`, and the app side completes them without a call) |
- **Impact**: low. It needs a failed subscribe followed by group activity on the same consumer.
  Then the callbacks reach a listener Java no longer calls (row 1), arrive unpaired, assigned
  without the matching revoked (row 2), or do not run at all (row 3). The bindings follow the core:
  the Python binding keeps the failed subscribe's listener context alive until the consumer is
  destroyed (`finding-1` commits), so making the core Java-faithful does not turn that path into a
  use after free.
- **State**: needs owner (core change outside this run). Found by Critic 120 round 6, item 2;
  rows 2 and 3 and the reproduction by Critic 120 round 7, item 2.

### 3. gRPC-backed consumer `close(CloseOptions)` overruns its timeout when every broker is down

- **Scenario**: `consumer_bounce_test::test_async_close` (Java `ConsumerBounceTest#testAsyncClose`,
  body `#testClose`), its last part `checkCloseWithClusterFailure`:
  - two consumers have each received 10 records; `consumer1` is subscribed (group `group4`),
    `consumer2` is manually assigned with `request.timeout.ms=6000` (group `group5`);
  - every broker of the dedicated 3-broker cluster is stopped;
  - `consumer1.close(CloseOptions.timeout(2000 ms))` must return in less than 2000 + 2000 ms;
  - `consumer2.close(CloseOptions.timeout(Long.MAX_VALUE ms))` must return in less than
    6000 + 2000 ms, Java's `request.timeout.ms` plus its 2 s `closeGraceTimeMs`.
- **Arms**: every gRPC arm, intermittently, on images with fdd2df838. The counts are of completed
  runs; one more run never started, because Docker had run out of network pools:
  - `__grpc_python`: 3 of 5 failed;
  - `__grpc_c`: 1 of 3 failed;
  - `__grpc_python_async`: 1 of 3 failed;
  - `__rust`: 0 of 4 failed, and the test passed before the conversion too.

  In each failing run one of the two closes overran:
  - `consumer2`'s unbounded close, on every arm that failed: `python backend: Close took too long
    8001` (twice, once after `consumer1` closed in 2098 ms), `c backend: Close took too long 8001`
    (after `consumer1` closed in 2006 ms), `python_async backend: Close took too long 8002`;
  - `consumer1`'s 2000 ms close, once, on `__grpc_python`: `python backend: Close took too long
    4001`.

  4001 and 8001 are not a fixed step of 2 s: they are the test's own limit firing,
  `tokio::time::timeout(ms + CLOSE_GRACE_TIME_MS)` in `close_and_validate`. Those closes took at
  least 2 s past their bound; how long they would have taken is unknown.
- **Java / Rust**: `close(CloseOptions)` with the cluster down returns within the close timeout,
  and with an unbounded timeout within `request.timeout.ms`
  (`AsyncKafkaConsumer#close(Duration)` bounds each close step by the timer). The native client
  returns in time on every run.
- **Arm**: the bindings' close sometimes returns 2 s or more past the bound. The C and both
  Python arms fail and the native one does not. The servers pass the timeout exactly (a
  `timedelta` since fdd2df838, clamped to `timedelta.max` for `Long.MAX_VALUE`) and add no wait of
  their own.
- **Diagnosis (inferred, not confirmed: from reading the code, nothing was run)**:
  - The sync Python wait adds no fixed delay. `_run_sync` loops `event.wait(0.1)` plus `_drain`,
    and the notify hook sets the event (`python/confluent_kafka/consumer/_base.py` `_run_sync`,
    `_on_pending_notify`; `python/_confluentkafka.c` `consumer_callbacks_notify`,
    `Consumer__execute_callbacks`). There is no 2 s constant on the path, and `_finish_close`
    waits only while another use of the handle is held. So the extra time is not in the binding's
    waiting.
  - Leading hypothesis: in container mode the backends reach the brokers by container hostname,
    and a reconnect resolves it again (`tokio::net::lookup_host`). Once `docker stop` has removed
    the name from Docker's DNS, a lookup can take seconds. The core's close step that joins the
    `kafka-consumer-io` thread then waits, unbounded, for a lookup still in progress: the network
    poll cannot be cancelled during resolution, and the drop of the io runtime waits for it. The
    native arm uses `127.0.0.1:<port>` (the host listeners are advertised and bootstrapped as IP
    literals), so no lookup runs and it is not exposed. That fits the evidence: every container arm
    fails sometimes, the native one never.
  - Java waits the same way: `ConsumerNetworkThread#closeInternal` wakes the thread and `join()`s
    it with no bound, and Java resolves on that thread too (`NetworkClient#initiateConnect` ->
    `ClusterConnectionStates#currentAddress` -> `ClientUtils#resolve`, again on each reconnect,
    since `ClusterConnectionStates#connecting` empties the address list). What differs is that the
    JVM caches `InetAddress` lookups (by default 30 s for a name found, 10 s for one not found),
    while the core runs a fresh `tokio::net::lookup_host` each time (`default_host_resolver.rs`).
    So "bound the io-thread exit in the core" would be a deviation from Java's unbounded `join()`.
    Whether this is a core divergence (no DNS cache) or an environment effect Java's cache mostly
    hides stays open: the confirmation run after batch 5 decides it. The other fix candidate is to
    make a stopped broker's name fail fast in the harness.
  - Suggested measurement: timestamps around the close submit, its completion, `_finish_close` and
    the destroy; `RUST_LOG=debug` for `client_utils` and the network thread; each container arm
    run about 10 times.
- **Failing assertion**:
  `assert!(time_taken_ms < ms + CLOSE_GRACE_TIME_MS, "{backend} backend: Close took too long {time_taken_ms}")`,
  reached through the bounding `tokio::time::timeout`'s
  `panic!("{backend} backend: Close took too long {}", ...)` in `close_and_validate`.
- **State**: skipped in the body on the gRPC arms (e8370c9b3; the `__rust` arm runs); needs owner
  (the diagnosis to confirm).
