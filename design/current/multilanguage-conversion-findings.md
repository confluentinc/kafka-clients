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
- **State**: fixed on this branch, in the `finding-1: ` commits; a separate PR is to follow (owner
  ruling 1). The binding keeps the earlier `ListenerCtx` after a `KafkaConsumer`'s `subscribe([])`
  and drops the new one; the C header texts and the Python `subscribe` docstring state the target
  release point. A `MockConsumer` is unchanged: Java's mock registers the listener on every
  `subscribe`, empty topics included (`SubscriptionState#subscribe`), so its `subscribe([])` does
  replace it. Regression tests: `python/test/integration/test_listener_release_broker.py` and
  `test_empty_subscribe_keeps_the_listener_until_destroy` in `c/tests/test_consumer_callbacks.c`.
  The scenario's un-revert is Actor 121's. Found by the Java-parity change of COMMENTS.121 item 5
  (owner decision 4, approved); the previous form of the test registered no listener and so never
  reached it.

### 2. Rust core: after a failed `subscribe`, a `KafkaConsumer` keeps calling the previous listener

- **Scenario** (described; no converted test exercises it): `subscribe(pattern, L1)` (or
  `subscribe([t1], L1)`), then a `subscribe([t2], L2)` that fails after the background thread
  processed its `TopicSubscriptionChangeEvent`, for example with the mixed-subscription-type
  `IllegalStateException` after an `assign`, or with the event's timeout while the background
  thread still processes it. Then a rebalance callback, or `close()` of a group member.
- **Arms**: every arm, since the core decides it (`__rust`, and the bindings over it).
- **Java 4.4**: one slot. `ApplicationEventProcessor#process(TopicSubscriptionChangeEvent)` calls
  `SubscriptionState#subscribe(Set,Optional)`, which runs `registerRebalanceListener(listener)`
  before `setSubscriptionType` throws, so after the failure the slot holds L2 (or nothing, for a
  listener-less `subscribe([t2])`). Every invocation reads that slot:
  `ConsumerRebalanceListenerInvoker` (`subscriptions.rebalanceListener()`),
  `AsyncKafkaConsumer`'s `PartitionsAssignedEvent` handling and `runRebalanceCallbacksOnClose`
  through the invoker. So later callbacks go to L2, and after a failed listener-less subscribe
  `close()` calls no listener.
- **Rust**: two slots. The background task runs the same `SubscriptionState::subscribe_with_topics`
  (register first, then the type check), so its slot holds L2, but the app side invokes the
  mirror `AsyncKafkaConsumer::rebalance_listener`, which `subscribe_internal_topics` and
  `subscribe_to_regex` write only after the event completed `Ok`. After the failure the mirror
  still holds L1: the background task queues the callback events (its slot `is_some()`), and the
  app side runs them, and `run_rebalance_callbacks_on_close`, on L1.
- **Impact**: low. It needs a failed subscribe followed by group activity on the same consumer.
  When it happens, the callbacks reach a listener Java no longer calls, and a listener-less
  failed subscribe leaves `close()` calling L1 where Java calls none. The bindings follow the
  core: the Python binding keeps the failed subscribe's listener context alive until the consumer
  is destroyed (`finding-1` commits), so making the core Java-faithful does not turn that path
  into a use after free.
- **State**: needs owner (core change outside this run). Found by Critic 120 round 6, item 2.
