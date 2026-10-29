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
- **Java / Rust**: `subscribe(emptyList)` is `unsubscribe()`, which keeps the subscription's
  rebalance listener. `close()` then runs `runRebalanceCallbacksOnClose` on that same listener: the
  native arm and the C arm log `assigned`, `revoked` and then `lost` (2 partitions, from the group
  assignment snapshot) and `close()` returns `Ok`.
- **Arm**: apparently the binding drops its reference to the listener when the listener-less
  `subscribe(topics=[])` runs, but the core still holds the listener and calls it from `close()`.
  - Python sync: the server process dies with `SIGSEGV` (container exit 139) inside `close()`; the
    test sees `python gRPC backend transport error (Unknown): transport error`.
  - Python asyncio: the core logs `User provided listener failed on invocation of
    onPartitionsLost ...: LocalCallbackError: 'function' object has no attribute '_on_lost'` (the
    pointer now refers to another Python object), and the test fails with
    `python_async backend: consumer close should succeed: Failed to close kafka consumer`.
- **State**: reverted to Rust-only (`// MULTILANGUAGE: blocked by finding #1`). The binding's
  listener lifetime must follow the core's (released when the core releases it, not on the next
  `subscribe`). Found by the Java-parity change of COMMENTS.121 item 5 (owner decision 4 pending);
  the previous form of the test registered no listener and so never reached it.
