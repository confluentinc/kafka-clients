---
name: Phase 7c review patterns
description: KafkaProducer skeleton review — public-ctor-deferral verification chain, async-fn→`+Send` cross-trait change, Drop-with-spawn pattern, test-name-overclaim Nit
type: project
---

Phase 7c reviewed: KafkaProducer skeleton (struct + ctors + Sender
spawn + Drop). 0 Blocking, 4 Suggestion, 2 Nit.

## Verifying a "deferred public ctor" is genuinely unavoidable

When the Actor reports "the public ctor returns `Err(UnsupportedOperation)`
because Phase X dependency Y isn't translated", **don't take this at
face value**. Walk the Java call chain to see if Y is actually required.

Phase 7c case: actor said "DefaultMetadataUpdater required". Verification
chain:

1. `KafkaProducer.java:454`: `this.sender = newSender(logContext, kafkaClient, this.metadata);`
2. `KafkaProducer.java:515-524`: `client = ClientUtils.createNetworkClient(producerConfig, …)`
3. `ClientUtils.java:163-176`: 10-arg overload passes `null` for `MetadataUpdater`.
4. `NetworkClient.java:317-325`: `if (metadataUpdater == null) this.metadataUpdater = new DefaultMetadataUpdater(metadata);`

Conclusion: yes, the production path constructs `DefaultMetadataUpdater`
implicitly. The `ManualMetadataUpdater` in the Rust repo doesn't drive
metadata refresh, so substituting it would silently break leader
discovery on topics not seeded at bootstrap. Deferral is correct per
CLAUDE.md rule 5.

**How to apply**: when reviewing a "deferred until X is translated"
claim, the verification chain is: producer ctor → newSender →
createNetworkClient overloads → NetworkClient ctor null-handling →
inner-class instantiation. If the chain bottoms out at a genuine
package-private dep that's not translated, defer is OK. If it bottoms
out at something already translated, the actor took an unnecessary
shortcut.

## Cross-trait `async fn` → `fn -> impl Future + Send` change

When a spawn-into-`tokio::spawn` task calls `client.poll(…).await`, the
spawned future must be `Send`. `async fn poll(…)` in trait declarations
desugars to a future that is **not** guaranteed `Send` even if the body
is — the auto-trait inference doesn't propagate through the
`#[allow(async_fn_in_trait)]` desugar. Fix:

```rust
// Before
async fn poll(&mut self, …) -> …;

// After
fn poll(&mut self, …) -> impl Future<Output = …> + Send;
```

Existing impls that use bare `async fn` syntax compile unchanged
because their bodies already produce `Send` futures. The change just
**hoists the `Send` bound from implicit-and-fragile to
explicit-and-enforced**.

**No-regression check**: walk every impl of the changed trait. A method
body that previously held a `MutexGuard` across an `.await` would have
already failed `Send` (so it was already broken or the caller never
spawned it). The new bound just promotes the requirement from runtime-
discovered to type-checked.

**Risk to flag**: if a future call site uses `async fn foo(&self) { let
g = self.mu.lock().await; bar.await; }` and the impl satisfies the
trait, the build will fail with a confusing "future not Send" error
pointing at the trait boundary. Recommend the Actor leave a comment at
the trait pointing at the cause.

## Drop-with-spawned-Tokio-task pattern

`Drop` is a sync context — cannot `.await` a `JoinHandle`. The accepted
pattern:

1. Capture `Arc<AtomicBool>` handles for `running` and `force_close`
   **before** moving the worker into `tokio::spawn`. (Methods like
   `running_arc()` / `force_close_arc()` on the worker that return the
   inner `Arc` cheaply.)
2. Store both Arcs on the producer struct.
3. Store the `JoinHandle<()>` as `Option<JoinHandle<()>>` so `Drop` can
   `take()` it.
4. In `Drop`: flip `force_close=true`, then `running=false` (in this
   order so the loop's drain stage exits via the force-close branch),
   then call `JoinHandle::abort()`.
5. The worker's run loop must `.await` something sleep-like so the
   loop yields back to the Tokio runtime — otherwise the flag flips
   are never observed.

Phase 7e adds an async `close()` that awaits the JoinHandle gracefully;
in steady state callers should call `close().await` before drop.

## Test-name-overclaim anti-pattern (recurring, see also 6e Round 2)

A test called `drop_aborts_sender_task` that only asserts
`running == false` after drop is **not actually testing the abort** —
it's testing the flag flip, which the Drop body does unconditionally.
To verify the abort, the test must capture a counter or completion
channel from the spawned task and check the task observed shutdown,
**or** capture the JoinHandle externally and
`tokio::time::timeout(…, handle).await` that the task completed.

This is the same anti-pattern as Phase 6e Round 2's "catch-unwind test
that closes the loop first → wrapped code never runs". The test passes
trivially because the assertion is satisfied by an unrelated body
statement.

**How to flag**: when a test name promises behavior X but the assertion
only checks behavior Y (where Y is a side-effect of the same code that
would also produce X), recommend either renaming the test (truthful) or
extending it to actually verify X.

## Field-by-field parity audit pattern

For a "skeleton" phase that ports the field-init block of a Java class,
the review's per-area table should explicitly enumerate the Java fields
in their declaration order and tag each as `present / missing / stubbed`.
Stubbed fields need a rationale (skip-list reference, milestone deferral,
out-of-scope marker). Missing fields without rationale are **bugs**.

Fields acceptable as stubbed in a skeleton phase:
- Metrics-related (`errors`, `producerMetrics`, `metrics`,
  `clientTelemetryReporter`) — out of milestone.
- `transactionManager` when Milestone-1 rejects `transactional.id` at
  config validation time.
- `partitioner` when always `None` and the trait-object instance is
  threaded by Phase 7d's builder API (rustdoc + NOTES.md note required).

## NETWORK_THREAD_PREFIX as observability hook

Java sets `ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId`.
Tokio doesn't have native task names; the Rust analogue is wrapping
the spawned future with `tracing::info_span!("kafka-producer-network-thread", client_id = %client_id)`
and `.instrument(span)`. Defining the constant `pub const NETWORK_THREAD_PREFIX`
without using it anywhere is a Suggestion-level dead export until the
tracing wiring lands.

## logUnused() parity check

Java constructors typically end with `config.logUnused()` (line 458 in
KafkaProducer). Rust translations frequently miss this. Not blocking
but flag as Suggestion — config-typo discovery is a real operator
benefit.

## When silent-bumps are silent in Java but logged in Java

Java `configureDeliveryTimeout` silently bumps `delivery.timeout.ms` to
`linger + request_timeout_ms` when the user didn't set it explicitly,
**and emits a `log.warn(…)`** so operators can see the bump. The Rust
translation that skips the warn line is a behavioral divergence
affecting observability — Suggestion-level. Watch for this whenever a
Java method has both a `throw` branch (when explicit) and a silent-bump
branch (when default): the silent-bump usually emits a warn that's easy
to miss.
