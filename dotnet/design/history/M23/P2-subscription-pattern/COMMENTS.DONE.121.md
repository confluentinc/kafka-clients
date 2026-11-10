# COMMENTS.DONE.121 — M23/P2, pattern subscribe (.NET Critic)

## S1 (f14f6a04, 0dd32dd1)

1. **`Equality_ComparesThePattern` cannot tell value equality from reference equality** —
   `dotnet/tests/Confluent.Kafka.UnitTests/PublicSubscriptionPatternTests.cs:39-45`.
   Both sides are built from the literal `"t.*"`, which C# interns per assembly, so `pattern` and `same` hold the
   *same* string instance; `""` (`string.Empty`, the `EmptyPattern_IsStoredAsGiven` case) and `null` are singletons
   too. Java's `equals` is `Objects.equals(pattern, other.pattern)`, i.e. `String.equals` (value), and the plan's S1
   case is "equal and unequal patterns with their hashes", but no case in the class reaches a second, equal-valued
   string instance. Proven by re-injection in a `git archive` tree (net10.0, `--filter
   FullyQualifiedName~PublicSubscriptionPatternTests`): control 5/5 passed; the mutant
   `Equals => obj is SubscriptionPattern other && ReferenceEquals(_pattern, other._pattern)` together with
   `GetHashCode => … RuntimeHelpers.GetHashCode(_pattern)` (identity hash) builds with 0 warnings and still passes
   5/5. With the fix below the same mutant fails `Equality_ComparesThePattern` (1 failed / 4 passed), and the fixed
   test passes 5/5 on HEAD's `SubscriptionPattern`.
   **Fix:** build one side from a runtime string, e.g. line 41
   `SubscriptionPattern same = new SubscriptionPattern(new string("t.*".ToCharArray()));` (not `"t" + ".*"`, which
   the compiler folds back into the interned literal), optionally with `Assert.NotSame(pattern.Pattern(),
   same.Pattern())` so the precondition is explicit; the `Equals` and hash assertions at 43-45 then exercise the
   ordinal value comparison and hash. (Leave the empty case as is: `new string(new char[0])` also returns
   `string.Empty`.)

   Resolved in f2566bc9.

## S2 (f2566bc9, b2bb8368)

1. **minor — a listener-less pattern subscribe's release of the previous listener is unpinned** —
   `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs:562` (sync `Subscribe(SubscriptionPattern)`,
   `SettleListener(null, subscribed: true)`) and `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.Async.cs:150`
   (async `SubscribeWithCallback(SubscriptionPattern, …)`, `failed => SettleListener(null, !failed)`); the test meant
   to cover them is `dotnet/tests/Confluent.Kafka.UnitTests/PublicConsumerPatternSubscribeTests.cs:154-185`.
   The interface docs promise "A replacing subscribe releases the previous listener, a listener-less
   `Subscribe(SubscriptionPattern)` included" (`IConsumer.cs:150-151`, `IAsyncConsumer.cs:205-206`), and the plan's S2
   case is "then a plain pattern subscribe releases it". The test only shows that the second `Rebalance` no longer
   reaches the listener, which the core gives on its own: any subscribe replaces the core's listener, whatever the
   binding does with its `ListenerRegistration`. Proven in a `git archive` tree of b2bb8368: the mutant that deletes
   both lines above (line 562, and the settle lambda at 150 replaced by `failed => { }`) builds 0 warnings and passes
   the **full** net10.0 suite, 7146 passed / 2 skipped / 7148, the same as the unmutated control. The old
   registration (and its `GCHandle` on the user's listener) then stays in `_listenerRegistration` until the next
   listener subscribe or `Dispose`. The topics forms pin the same promise with
   `Interop/ConsumerRebalanceListenerBridgeTests.cs:246` and `:261` (`IsReleased`, `CurrentListenerRegistration`).
   **Fix:** add the two pattern twins of those tests, e.g.
   `consumer.Subscribe(new SubscriptionPattern("t.*"), listener)` → keep `CurrentListenerRegistration` →
   `consumer.Subscribe(new SubscriptionPattern("t.*"))` → `Assert.True(registration.IsReleased)` and
   `Assert.Null(consumer.CurrentListenerRegistration)`, and the same over `SubscribeWithCallback`. Probed: both pass
   on HEAD (2/2) and both fail under the mutant (0/2, `Assert.True() Failure`). Unit count +2 per TFM.

   Resolved in 3f820e1d.

2. **minor — a pattern holding U+0000 is cut at the NUL with no error (ruling needed)** —
   `dotnet/src/Confluent.Kafka/Internal/Interop/NativeCallArguments.cs:57-62` (`SubscriptionPattern` pins
   `Utf8(pattern)`, a NUL-terminated buffer, for `kafka_consumer_SubscriptionPattern_new_err(const char *pattern, …)`,
   whose `require_str` reads up to the first NUL). A new `const char *` site. Probed on HEAD's native: a
   `NativeCallArguments.SubscriptionPattern("a\0b")` handle reads back through `kafka_consumer_SubscriptionPattern_pattern_err`
   as `"a"` with `out_err` NULL, and `MockConsumer.Subscribe(new SubscriptionPattern("a\0b"))` returns normally, so
   a `KafkaConsumer` would send the broker another regular expression. Java sends the string whole; Python refuses it
   with `ValueError("embedded null character")` (`PyArg_ParseTuple(args, "KzOp", …)` / `"KzOpO"`,
   `python/_confluentkafka.c:6165`, `:6191`). The binding already rejects a NUL on the producer's topic and header
   keys (ledger row 314) and in admin strings, with `ArgumentException` (`Internal/AdminStrings.cs:21-49`); the
   consumer's topics `Subscribe` has the same pre-existing gap (`StringList` → `Utf8`). **Fix (for a ruling, not
   necessarily in S2):** (a) reject a pattern containing `'\0'` with `ArgumentException("pattern")` worded as
   `AdminStrings` words it ("A subscription pattern must not contain a NUL character: …"), where Python raises (after
   the null and `Pattern()`-null checks), documented on both interfaces and recorded as a deviation from Java, with
   the topics forms in the same change or as a follow-up; or (b) record the truncation.

   Resolved in da78ea8f.

## S3 (6beb599e)

Notes for the comment-only commit (S4):
- `dotnet/grpc-server/ConsumerServiceImpl.cs:95`, `dotnet/grpc-server/AsyncConsumerServiceImpl.cs:118`: the added
  clause "A request with a pattern subscribes with it, else with its topics" restates the `if` (rule 3); the
  listener-release half is the decision.

  Resolved in 8596bc44.

## S4–S5 (8596bc44, 670eb800)

1. **minor — the Flakes line says both timeouts were in files the phase did not touch; one was in a touched file** —
   `dotnet/design/current/STATUS.md:15`. The consumer case is
   `PublicConsumerCallbackRoutingTests.InsideACallback_EachCall_IsRoutedToTheHandle_RefusedAtOnce_OrSentToTheCore`
   (vehicle "AsyncKafkaConsumer, an async commit callback after an await"), which timed out in `CloseAndDispose` at
   3f820e1d's net10.0 gate (scratch `i1-unit-net10.0.log`, 7150 total; full re-run passed). b2bb8368 changed that file
   and that very case: `ProbeAsync` (`PublicConsumerCallbackRoutingTests.cs:982-983`) now runs `Subscribe(pattern)`
   and `Subscribe(pattern, listener)` inside the callback before the teardown. The phase is not a plausible cause (the
   pattern forms are refused by the same `RefuseInsideACallback` in `CheckAfterAcquire`, before any native call or
   listener registration, as the topics probes), but the record says otherwise. The producer case
   (`PublicProducerSerializerFailureTests`, da78ea8f's net8.0 gate) is untouched, as written; the two were at two
   different fixup gates, not one. **Fix:** e.g. "At the S2 fixups' gates, two teardown timeouts passed on re-run: a
   consumer close-and-dispose case in `PublicConsumerCallbackRoutingTests` (3f820e1d, net10.0; the phase added the two
   refused pattern-subscribe probes the case runs before its teardown) and `PublicProducerSerializerFailureTests`'
   null-key case (da78ea8f, net8.0; untouched)."

   Resolved in the fixup of 670eb800.

2. **minor — "12 members:" is followed by a list of 15** — `dotnet/design/current/STATUS.md:12`. Two overloads on
   `IConsumer`, `KafkaConsumer`, `MockConsumer` and three on `IAsyncConsumer`, `AsyncKafkaConsumer`,
   `AsyncMockConsumer` are 15 public members (grep of the six files); 12 is the number of Java members they cover
   (the 12 baseline lines removed), the other 3 being the allowlisted async-listener forms (row 414). PLAN.md:189's
   "the 12 members" is the source. **Fix:** "15 members (Java's 12, plus the async-listener form, row 414): …".

   Resolved in the fixup of 670eb800.

## PR #326 Copilot (ledger row 431, Agreed: option (a)) — approved for the Actor

1. **must-fix — a refused subscribe frees a listener the core still holds** —
   `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs` `SubscribeWithListener` / `SettleListener` (~576-640) and
   `NativeConsumer.Async.cs` `SubscribeWithRegistration` (~205-260). On a failed native subscribe the binding releases
   the new `ListenerRegistration` (its `GCHandle`), but the core registers the listener before its subscription-type
   check (`rust/src/consumer/internals/subscription_state.rs` `subscribe_with_topics` / `subscribe_with_pattern`:
   `register_rebalance_listener` then `set_subscription_type(...)?`, as Java's `SubscriptionState.subscribe`), so a
   refused switch (a mock subscribed to topics, then `Subscribe(pattern, listener)` → "…mutually exclusive"; or
   `Assign`, then `Subscribe(topics, listener)`) leaves the core with a freed handle, which a later `Rebalance` calls.
   The C contract (`kafka_consumer_Consumer_subscribe_with_topics_listener` rustdoc) keeps the listener's `self` alive
   "until the next `subscribe_*`, `unsubscribe` or the consumer's destruction releases it", with no exception for a
   failed call. Pre-existing in the topics forms. **Fix (row 431 (a)):** never release a registration the core may still
   hold: after a failed subscribe keep the new registration and the current one alive until a later successful
   subscribe or the consumer's destruction, and release them there; the bookkeeping of the current registration
   follows what the core holds after a refused switch (the new one); a successful subscribe still replaces and
   releases, `Unsubscribe` and `Close` keep it; nothing changes when a subscribe succeeds. Not allowed: a .NET-side
   subscription-type check (b), or any Rust/C/Python change (c).

   Resolved in 8e46cfb4.

## Row 431 fix (8e46cfb4, bfda6a25)

1. **minor — a refusal asserted by type only** —
   `dotnet/tests/Confluent.Kafka.UnitTests/Interop/ConsumerRebalanceListenerBridgeTests.cs:487-488`
   (`Dispose_AfterARefusedSubscribe_ReleasesEveryRegistration`): `Assert.Throws<IllegalStateException>(…)` drops the
   exception, where every sibling asserts `MutuallyExclusive` (PLAN §7's lens, `definition-of-done.md` §3). **Fix:**
   `IllegalStateException refused = Assert.Throws<IllegalStateException>(…); Assert.Equal(MutuallyExclusive,
   refused.Message);`. While there, the test is the only new one without `using IDisposable consumerDisposal =
   TestTimeout.Disposal(consumer);`, so a failing assertion before line 492 leaves the mock to its finalizer; adding
   it (a second `Dispose` is harmless, as `OnAKafkaConsumer_ARefusedSwitch` does) keeps the sibling shape.

   Resolved in 8c516ae1.

2. **needs-ruling — the rulebook still says a failed subscribe frees the new listener** (the Actor's open point 1;
   a real inconsistency, not a defect of the fix). `dotnet/.claude/rules/ffi-marshalling.md:2236-2238` reads
   "free the old listener's `GCHandle` when a replacing `subscribe_*` succeeds, the new one when the `subscribe_*`
   fails, and the current one after the consumer's destroy has returned." and `:2339-2342` (tests required) reads
   "a replacing subscribe frees the old `self_`; a failed subscribe frees the new one; an `unsubscribe` frees
   nothing; destroy frees the current one after it returns". Both contradict row 431 (a) and the code now pinned by
   the rewritten tests. **Proposed** (`:2236-2238`): "free the old listener's `GCHandle`, and every one a failed
   `subscribe_*` left, when a replacing `subscribe_*` succeeds. A `subscribe_*` that reported an error may already
   have registered its listener (the core's `SubscriptionState` registers it before the subscription-type check, as
   Java's), so the new one becomes the current one and the one it replaced is kept; a failed listener-less
   `subscribe_*` changes nothing (row 431 (a)). Free a new one at once only when no `subscribe_*` call was made, and
   the current and every kept one after the consumer's destroy has returned." **Proposed** (`:2339-2342`): "a
   replacing subscribe frees the old `self_`; a refused subscribe frees neither, its listener is the one a mock's
   next rebalance fires, and the next successful subscribe frees both; an `unsubscribe` frees nothing; destroy frees
   the current and every kept one after it returns". (`:1135-1138`, `:1538`, `:1689` and `:2177` only point at
   decision 23 and stay true.)

   Resolved in 5c9066dc (ledger row 434).

3. **needs-ruling — the public docs say a listener fires until a subscribe that *succeeds* replaces it** (the
   Actor's open point 2; a real inconsistency, older than this fix: the topics wording is from `cd83931b`, the
   pattern one from `ed21c093`). Current summary sentence, six overloads (`IConsumer.cs:87-88`, `:145-146`;
   `IAsyncConsumer.cs:104-105`, `:138-139`, `:200-201`, `:248-249`): "The listener fires on every later rebalance
   until a subscribe that succeeds replaces it."; current remark (`IConsumer.cs:91-92`, `:154-155`;
   `IAsyncConsumer.cs:108-110`, `:209-210`): "A replacing subscribe releases the previous listener, a listener-less
   … included, as does disposing the consumer." Facts: on `MockConsumer` / `AsyncMockConsumer` a refused subscribe
   replaces the listener (with its own, or with none when listener-less), so the next `Rebalance` fires the refused
   one, which the new `OnTheMocks_*` tests pin; that is Java's behaviour on both of Java's consumers
   (`SubscriptionState.subscribe` registers before `setSubscriptionType`, and `ConsumerRebalanceListenerInvoker`
   reads `subscriptions.rebalanceListener()`). On the .NET real consumers the core fires its app-side mirror, written
   only on success, so after a refused switch with a listener the old one keeps firing, while after a refused
   listener-less subscribe the core gates the revoked/lost events on `SubscriptionState`'s (now empty) listener and
   still fires assigned from the mirror (`abstract_membership_manager.rs`
   `enqueue_rebalance_callback_with_ack_through_application_events` vs `enqueue_partitions_assigned_event`): a core
   divergence from Java, for the core owners, not this binding. **Proposed** summary: "The listener fires on every
   later rebalance until another subscribe replaces it." **Proposed** remark: "A subscribe the consumer refuses can
   replace it too: on the mocks, as in Java, the next rebalance fires the refused subscribe's listener, or none. A
   subscribe that succeeds releases the previous listener and those of the subscribes refused before it, a
   listener-less … included, as does disposing the consumer." (The `<see cref>` of each remark stays as it is.)

   Resolved in b829a6ba (ledger row 434).

## PR #326 Copilot comment A (thread PRRT_kwDOQjF7uc61MRCS, ledger row 436, Agreed: A1) — approved for the Actor

1. **must-fix — on a real consumer, an empty-topic subscribe frees registrations the core still holds** —
   `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs` `SettleListener` (~646-680) and its callers (sync ~538-549,
   ~582-627; async `NativeConsumer.Async.cs` ~112, ~150, ~240). On `KafkaConsumer` / `AsyncKafkaConsumer`, `Subscribe`
   with an empty topic list is Java's `unsubscribe()`: `rust/src/consumer/internals/async_kafka_consumer.rs`
   `subscribe_internal_topics` runs `ensure_open`, the `group.id` check, then `return self.unsubscribe().await` for empty
   topics (~4878-4887), dropping the new listener unused and leaving the app-side mirror (~4932) and `SubscriptionState`
   (`unsubscribe`, `subscription_state.rs` ~904-912) holding the old one. .NET settles a successful empty subscribe as a
   success and frees the replaced and every retained registration. Pre-existing for the topics forms; contradicts .NET's
   own `Unsubscribe`, which keeps the registration. The mocks register the listener on an empty list
   (`mock_consumer.rs` ~518-527 → `subscription_state.rs` ~712-720), as Java's `MockConsumer`. **Fix (row 436 A1):** on
   the real consumer only (the binding's mock flag), an empty-topic subscribe, in all four forms (sync and async, with
   and without a listener, both async listener kinds), is bookkept like `Unsubscribe`: nothing the binding holds is
   released; a listener-bearing form's new registration is released once the native call has returned (sync) or
   completed (async); if the core never stores the new listener on the empty path whatever the outcome (verify), this
   applies on success and failure alike. Mocks unchanged; non-empty subscribes unchanged. Decide from the caller's input
   (empty topics) and the consumer kind only; no .NET copy of a core check; no Rust/C/Python change. Comment B
   (pre-registration failures retained, `NativeConsumer.cs:666`) is NOT ruled: do not change the failure path for it.

   Resolved in 53f6fef9 / 61c2ca32.

## PR #326 Copilot comment B (thread PRRT_kwDOQjF7uc61MRDM, ledger row 437, Agreed: B1) — approved for the Actor

1. **must-fix (leak) — a subscribe refused before the core holds its listener still retains it** —
   `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs` `SettleListener` (~646-690) and the sync / async callers.
   Since the row 431 fix every listener subscribe whose native call was made and failed retains its registration until
   a later successful subscribe or the consumer's destruction, also when the core refused it before the listener reached
   `SubscriptionState`: closed / bootstrap fatal (`async_kafka_consumer.rs` ~4823, ~4880), missing `group.id` (~4881,
   ~4943), blank topic (~4889-4895), empty pattern (~4944-4946), concurrent use (FFI acquire,
   `rust/src/ffi/consumer/mod.rs` ~326-333). Retries pile up; without `group.id` only `Dispose` frees them.
   **Fix (row 437 B1):** for a listener-bearing subscribe whose native call was made and failed, classify by the FFI
   error id: `LocalIllegalArgument`, `InvalidGroupId` or `LocalConcurrentModification` → release the new registration at
   once and change nothing else (the previous one stays current, the retained list unchanged); every other failure keeps
   row 431's rule (the new one becomes current, the replaced one is retained) — `LocalIllegalState` (mutually-exclusive
   refusal, closed, a caught panic), `Timeout`, a bare `KafkaException`, a dropped completion sender, anything unknown.
   Success, a never-made call and row 436's empty-topic case are unchanged (where A1 and B1 overlap the outcome is the
   same). Verify first that each of the three ids is raised only before `register_rebalance_listener`, on the real
   consumer and the mock, sync and `_cb`, and that nothing after registration returns one; if not, STOP and do not ship
   B1. Use the binding's generated ids/constants, no hard-coded numbers where a constant exists.

   Resolved in 65862879 / 840216d4.

## Rows 436–437 (53f6fef9 … dfe502af)

1. **minor — the listener's own summary still says a listener-less subscribe releases it** —
   `dotnet/src/Confluent.Kafka/Consumer/IConsumerRebalanceListener.cs:52-54`: "A replacing subscribe, a listener-less
   one included, releases it, as does disposing the consumer; `Unsubscribe()` and `Close()` keep it". Since A1, on
   `KafkaConsumer` / `AsyncKafkaConsumer` a listener-less `Subscribe` to no topics keeps it
   (`OnAKafkaConsumer_AnEmptySubscribe_KeepsTheRegistration`), and since row 431 a refused replacing subscribe releases
   nothing; the four remarks A1 and B1 edited now say both, this paragraph says the opposite. The only other match of
   the sweep (`git grep -i "replacing subscribe\|listener-less one included"` outside the history) is a test message.
   **Fix:** "A listener is bound to a subscription and fires many times. A subscribe that succeeds, a listener-less one
   included, releases it, as does disposing the consumer; `Unsubscribe()` and `Close()` keep it, as Java's
   `SubscriptionState.unsubscribe()` does, and so does a subscribe to no topics on a `KafkaConsumer` or
   `AsyncKafkaConsumer`, which is `Unsubscribe()`." (keeping the paragraph's `<c>` markup).

   Resolved in cd89e490.

2. **minor — A1's failure half is pinned only through B1's overlap** —
   `dotnet/tests/Confluent.Kafka.UnitTests/Interop/ConsumerRebalanceListenerBridgeTests.cs:602`
   (`OnAKafkaConsumer_AFailedEmptySubscribeWithAListener_ReleasesItsRegistration`) is the only failed empty-topic case,
   and its id is 24, which B1 now releases on its own. The mutant that applies A1 only on success,
   `NativeConsumer.cs:621` → `notRegistered = (unsubscribes && subscribed) || KafkaException.RefusedBeforeRegistering(outErr);`
   and `NativeConsumer.Async.cs:246` → `(unsubscribes && error == IntPtr.Zero) || KafkaException.RefusedBeforeRegistering(error)`,
   builds 0 W and passes all 1380 `~Consumer` tests (net10.0). Its effect is a retention, not a free (the empty path's
   registration becomes current and the replaced one is kept), against the ruled "on success and failure alike".
   **Fix:** a case whose empty-topic subscribe fails with another id. A bootstrap-resolution failure (-29) is
   deterministic: a real consumer with `bootstrap.servers=unresolvable.invalid:9092`, `bootstrap.resolve.timeout.ms=1000`
   and a `group.id`, subscribed to a topic and polled (`Poll(100 ms)` in a bounded loop, as Python's
   `test_kafka_consumer.py` bootstrap test) until `BootstrapResolutionException`, then the empty-topic listener subscribe
   in the three forms: `BootstrapResolutionException` with the core's message, `CurrentListenerRegistration` null,
   `RetainedListenerRegistrationCount` 0, the listener collected. Probed (a subscribe loop instead of the poll loop):
   3 / 3 green at HEAD in each of 3 runs (~1 s a case), 3 / 3 red under the mutant (`Assert.Null`: a registration
   current). +3 cases per TFM.

   Resolved in cd89e490.

3. **minor — decision 23 gives -5 as a later refusal; the subscribe's timeout is 7** —
   `dotnet/.claude/rules/ffi-marshalling.md:2244` "Any other refused `subscribe_*` (-4, -5, a bare `KafkaException`, any
   other id)". -5 is `LOCAL_TIMEOUT` (Java's `java.util.concurrent.TimeoutException`), which nothing under
   `rust/src/consumer` or `rust/src/ffi/consumer` raises; the subscribe's timeouts are `Error::timeout` →
   `REQUEST_TIMED_OUT` = 7 (`rust/src/ffi/common.rs` `Error::Timeout(_) => kafka_common_Error_REQUEST_TIMED_OUT`, BH:222).
   The rule's effect is right ("any other id" covers 7), the example is not. **Fix:** "(-4, 7 for a timeout, -1 for a
   bare `KafkaException`, any other id)".

   Resolved in e29207eb.

## PR #326 Copilot comment C (thread PRRT_kwDOQjF7uc61VNW3, comment 4514883452, ledger row 441, Agreed: (A)) — approved for the Actor

1. **must-fix (narrow) — two overlapping async subscribes can settle out of order and free the listener the core holds** —
   `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.Async.cs` (~246, the subscribe settle) with the owner check in
   `NativeConsumer.cs` (~2677-2702, which refuses or serializes only another thread). On `AsyncKafkaConsumer` /
   `AsyncMockConsumer` a second Subscribe started on the same thread before the first settles passes .NET's check;
   the core's `run_cb` (`rust/src/ffi/consumer/mod.rs` ~724-735) takes the busy flag, then `drop(guard)` before
   `deliver(...)`, so the second can run and queue its completion first, and the FIFO pump settles in that order,
   freeing the second's live listener. Pre-existing (async topics-listener forms at `3afba0a7`). **Fix (row 441 (A)):**
   on the two async consumers, a Subscribe in any form (topics or pattern; with or without a listener; both async
   listener kinds; the empty-topic form) started while another Subscribe on the same consumer has not yet settled
   throws `ConcurrentModificationException` with the core's exact text (verify it; reuse the existing .NET constant)
   before calling C, creating no registration. "Settled" = the first subscribe's settle has run
   (`ConsumerCompletion.Complete`, before `SetOutcome`). Release the in-flight mark exactly once on every path (success,
   every failure incl. the core's own CME, `Submit` throwing before or after the native call, cancellation,
   Close/Dispose running pending callbacks); it must never stick. Subscribes only; sync consumers unchanged (confirm).
   A .NET guard for the core's gap, to go when row 442 lands; no Rust change. Tests deterministic only (no sleeps): if
   the first subscribe cannot be held unsettled by an existing hook, STOP and report.

   Resolved in 4dc2c457 / bcff7deb.

## Row 441 guard (4dc2c457, bcff7deb, 6876b1e4)

1. **minor — the six new `ConcurrentModificationException` entries are wrong on `AsyncMockConsumer` across threads** —
   `dotnet/src/Confluent.Kafka/Consumer/IAsyncConsumer.cs:100-103`, `140-143`, `172-175`, `213-216`, `269-272`,
   `313-316`. On the mock another thread's call, its `Subscribe` included, waits for this thread's use instead of
   throwing (`TryEnterUse`, `NativeConsumer.cs` ~2662-2717; refused only while `RefusedWhileBusy`, ~2748: an async
   callback runs or an async `Rebalance` / `LosePartitions` holds the window), and the commit's own
   `PublicMockConsumerOwnerWaitTests.AsyncMock_AnotherThreadsSubscribe_WaitsForThisThreadsSubscribe_ThenIsAccepted`
   pins exactly that, so the new clause "another `Subscribe` on this consumer has not completed" is false there (the
   first clause is the type remark's `AsyncKafkaConsumer` case, `IAsyncConsumer.cs:33-35`; the in-callback clause is
   right on both, the closed case being the `IllegalStateException` entry). **Fix:** in the six entries, "Another
   thread is inside a call on this consumer, the call is made inside a callback of this consumer, or another
   <c>Subscribe</c> made on this thread has not completed. On an <see cref="AsyncMockConsumer{TKey, TValue}"/> a call
   from another thread waits instead." (The Close entries at `1054-1057` / `1075-1078` share clause 1's shorthand;
   pre-existing, not asked.)

   Resolved in da5e36f1.

2. **minor — the release in `Submit`'s catch is unpinned** — `NativeConsumer.Async.cs:1142`. With that
   `EndSubscribe(subscribing)` removed, the new class and `PublicMockConsumerOwnerWaitTests` stay 162 / 162 green
   (archive of `6876b1e4`, net10.0). It is reached only when `Start` throws after the mark (the subscribe P/Invoke
   refusing the handle, `NewHandle` / `SubscriptionPatternNew` failing), which the public API cannot drive; the commit
   records "not reachable" only for the Dispose path, while STATUS states the release "when `Submit` throws". The
   settle's `finally` (`:1175`, a throwing settle) is likewise unpinned and unreachable. **Fix:** pin it (call
   `Submit<bool>` by reflection with `subscribes: true` and a `submit` that throws, then assert a real `Subscribe` is
   accepted; red on the mutant), or record both as not reachable through the public API beside the Dispose note.

   Resolved in da5e36f1.

3. **minor — decision 23's tests-required line has no case for the guard** — `dotnet/.claude/rules/ffi-marshalling.md`
   `2356-2363` ("**Listener:** …"). Rows 436 and 437 each added their case there in this phase; row 441's sentence
   (`2256-2259`) has none. **Fix:** append "on `AsyncKafkaConsumer` / `AsyncMockConsumer` a subscribe started while
   another has not settled is refused with -2's text before the call, with no registration, and the next is accepted
   once the first has settled, whether it succeeded or failed".

   Resolved in ba8452ff.

4. **minor — STATUS says "every form pair"** — `dotnet/design/current/STATUS.md:18`. The overlap theory holds the six
   non-empty forms first and all nine second (54 of 81 pairs per consumer), as the commit message says. The empty-topic
   form as the held first matters on the mock, which registers its listener (`Unsubscribes` is false there).
   **Fix:** say "each non-empty form held, every form second", or add the three empty forms to the first position (the
   final assertion then expects no current registration for them on `AsyncKafkaConsumer`).

   Resolved in da5e36f1 (every form held first); the STATUS line in the commit that archives this item.

## Rebase onto 09aa4888 (653bd2f2)

1. **minor**: `dotnet/design/current/STATUS.md:10` and `:12`. Besides the hashes, two facts in our entry describe the
   pre-rebase branch: "from `3afba0a7` (PR #325 head)" (now `09aa4888`) and "Six P/Invokes, 967 → 973" (now
   968 → 974, `NativeMethodsPrelinkTests.cs:32`). **Fix:** correct both in the Manager's hash refresh, and record the
   rebase gate once (prelink 974; baseline 505 → 493 entries, 12 removed; the UnitTests totals). The per-step gate lines
   (13, 15–18: prelink 973, "Wrote 942") are dated records and can stay. If `b95977a4` is kept as a commit, its
   message's "prelink 967 -> 973" should be fixed at the squash. PLAN.md's `3afba0a7` / 967 / 942 figures are the plan's
   dated record and need no change.

   Resolved in the fixup of 04808736 (STATUS refresh after the rebase).
