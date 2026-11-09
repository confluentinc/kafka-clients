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
