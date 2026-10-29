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
    onPartitionsLost ...: LocalCallbackError: 'function' object has no attribute '_on_lost'` (the
    freed adapter's memory now holds another Python object), and the test fails with
    `python_async backend: consumer close should succeed: Failed to close kafka consumer`.
- **The C contract states the same release point**: the doc of
  `kafka_consumer_ConsumerRebalanceListener_new` (`# Safety`: the caller keeps `self_` alive "until
  the next `subscribe_*`, `unsubscribe` or the consumer's destruction releases it"),
  `kafka_consumer_Consumer_subscribe_with_topics` ("Replaces the listener registered by an earlier
  `subscribe_*`, releasing its `self`") and `kafka_consumer_Consumer_unsubscribe` ("releases the
  registered listener"). The core keeps the listener after both `unsubscribe()` and
  `subscribe_with_topics([])`, and `close()` calls it whenever the member had partitions, so a C
  caller who frees `self_` where the header allows gets the same use-after-free. The C arm passes
  only because the C server's `self_` (its `LogState`) lives for the whole session and so outlives
  every release point; the arm never tests the release point. Fixing the binding alone would leave
  the C contract wrong.
- **State**: reverted to Rust-only (`// MULTILANGUAGE: blocked by finding #1`); needs owner (Python
  binding and C header fix, outside this run). Found by the Java-parity change of COMMENTS.121
  item 5 (owner decision 4, approved); the previous form of the test registered no listener and so
  never reached it.
