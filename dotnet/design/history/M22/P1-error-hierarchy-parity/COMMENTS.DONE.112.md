# Critic 112: review of Phase 3, building the right class (resolved)

Scope: the uncommitted Phase 3 changes on top of `70b48397c` (`git diff HEAD`, 251 files) and the two
new tests `ErrorHierarchyTests.cs` and `FromFfiErrorClassTests.cs`, reviewed against
`dotnet/design/history/M22/P1-error-hierarchy-parity/PLAN.md` (Phase 3; D4, D5, D9, D10, D11, D15, D18, D19,
D20, D21, D22, D24; §2, §7), `dotnet/CLAUDE.md` §2, §3, §4 and §7.6, ffi-marshalling §A5/§B5, the
header `rust/target/include/confluent_kafka_bindings.h` and the Java sources in `kafka/`.


## 1. Medium: the gRPC server's ListGroups still casts each error to `KafkaException`

- **Where:** `dotnet/grpc-server/AdminServiceImpl.cs:1161` and
  `dotnet/grpc-server/AsyncAdminServiceImpl.cs:1164`:

  ```csharp
  IReadOnlyCollection<Exception> errors = result.Errors().Get();
  ...
  foreach (KafkaException error in errors)
  {
      response.ListingErrors.Add(Translate.ToProto(error));
  }
  ```

- **Reference:** Java's `ListGroupsResult.errors()` is `KafkaFuture<Collection<Throwable>>`, and the
  plan's D9 retypes it to `IReadOnlyCollection<Exception>` so that a non-`KafkaException` can be
  reported. The Phase 3 bullet "D9 ... including the gRPC server's two `Errors()` sites" asks for
  both consumers to follow. The `ElectLeaders` sites (`Exception?`) were updated; this one changed
  only the variable's type.
- **Failure scenario:** `foreach (KafkaException error in IEnumerable<Exception>)` is an explicit
  cast in C#. A per-broker listing error that is now a D15 class, for example a core −4
  `IllegalStateException`, −3 `IllegalArgumentException` or −5 `java.util.concurrent.TimeoutException`,
  or any carried non-Kafka exception, throws `InvalidCastException` inside the loop.
  - The `catch (Exception ex)` that follows sends the whole RPC as
    `Error = ToProto(InvalidCastException)`, the fallback −4 "dotnet server: InvalidCastException: ...".
  - The `Valid` listings already gathered are lost.
  - Python's server reports each error in `ListingErrors` and keeps the listings.
  - Before Phase 3 these ids were `KafkaException`s, so this is a regression this phase introduces.
- **Fix:** `foreach (Exception error in errors)` at both sites (`Translate.ToProto` already takes
  `Exception`). Add an `AdminServiceTests` / `AsyncAdminServiceTests` case, or a `TranslateTests`
  case through the same loop, where `Errors()` holds a non-`KafkaException` (an
  `IllegalStateException`) and the response keeps its `Valid` entries with the error in
  `ListingErrors` under −4. A grep for `foreach (KafkaException` over `dotnet/` also catches any
  further explicit-type consumers of a retyped collection; these two are the only ones.

**Resolution (Actor 112):** both sites now read `foreach (Exception error in errors)`:
`dotnet/grpc-server/AdminServiceImpl.cs` and `dotnet/grpc-server/AsyncAdminServiceImpl.cs`. A grep
for `foreach (KafkaException` over `dotnet/` finds no other site. New test
`ListGroups_WithANonKafkaListingError_KeepsTheValidListingsAndSendsTheErrorAsMinusFour` (sync and
async) in `dotnet/tests/Confluent.Kafka.GrpcServer.UnitTests/AdminServiceTests.cs`. It uses the
new fixture `Fixtures/ListGroupsOverride.cs` (a `DispatchProxy` over the mock admin, wired by
`AdminServer.OverrideListGroups`), which makes `ListGroups` answer one valid listing and two
errors: an `IllegalStateException` and a plain `InvalidOperationException`. The test asserts that
the RPC has no `Error`, keeps the `g1` listing, and sends both errors in `ListingErrors` under −4:
"broker listing failed" and "dotnet server: InvalidOperationException: listing invariant broken",
as Python's server sends them. Mutation check: with the old loop the sync case fails.


## 2. Low: `KafkaConsumer.Subscribe` and `Assign` check the argument before the closed, owner and group checks

- **Where:** `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs`:
  - `Subscribe(topics)`: `SnapshotTopics` at 568, then `RefuseInsideACallback()` at 569, then
    `CallSync`, which makes the closed and owner checks;
  - `Subscribe(topics, listener)`: 582, before 583;
  - `Assign`: `SnapshotAssignPartitions` at 694, as an argument to `RunPartitionsSync`.

  The async peers are in `NativeConsumer.Async.cs`: `SnapshotTopics` at 96/97 and 141/142, and
  `SnapshotAssignPartitions` at 220.
- **Reference:** Java's `AsyncKafkaConsumer.assign` calls `acquireAndEnsureOpen()` before its null
  check, and so does `subscribeInternal`, which checks the group (`throwIfGroupIdNotDefined()`)
  before `topics == null`.
  - Python follows: `_assign_partitions` raises inside `self._use()`, and the `subscribe_topics`
    entry of `_base.py`'s check table is marked after the open check. The listener check is the only
    one before it, which .NET's `RequireListener` already gets right.
  - CLAUDE.md, Python Binding Conventions, Order: "Python checks the closed state first".
  - The binding's own `CheckWhenOpen` (`NativeConsumer.cs:938`) runs `ThrowIfClosed()` first for
    the commit and seek checks.
- **Failure scenario:** on a closed `KafkaConsumer` / `AsyncKafkaConsumer`, `Assign(null)` or
  `Subscribe(null)` throws `IllegalArgumentException` where Java throws `IllegalStateException`
  ("This consumer has already been closed."). A second thread gets `IllegalArgumentException`
  where Java gets `ConcurrentModificationException`. The ordering was there before this phase, but
  Phase 3 (D20) is what gives these checks Java's classes and messages, so a catch by class now
  differs from Java's. It is Low because the checks are on misuse paths only.
- **Fix:** run the KafkaConsumer branch of `SnapshotTopics` / `SnapshotAssignPartitions` through
  `CheckWhenOpen`, so the closed check runs first, as `SnapshotSeekPartitions` and the commit checks
  do. This leaves the owner and group-id order as the accepted Python deviation ("The Python null
  checks run before the core's own checks"). Add a test: `Subscribe(null)` and `Assign(null)` on a
  closed `KafkaConsumer` throw `IllegalStateException` with the closed message. Or record the order
  as a deviation in the plan's §7. Keep the mocks as they are, since Java's `MockConsumer` checks
  differently.

**Resolution (Actor 112):** the order is now Java's, not recorded as a deviation. The new helper
`CheckAfterAcquire` in `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs` runs the argument
checks after `RefuseInsideACallback()` and the closed and owner checks (`CheckInAUse`), as Java's
`acquireAndEnsureOpen()` does. Its call sites:
- `Subscribe(topics)`, `Subscribe(topics, listener)` and `Assign` in `NativeConsumer.cs`;
- `SubscribeWithCallback`, `SubscribeWithRegistration` and the async `Assign` in
  `NativeConsumer.Async.cs`.

`RequireListener` still runs first, as Java checks the listener before `subscribeInternal`. On a
mock the helper runs the checks alone, so the mocks keep their order. The group-id check before the
null check is still the core's, the accepted Python deviation.

New tests in `dotnet/tests/Confluent.Kafka.UnitTests/PublicConsumerCheckOrderTests.cs`, on
`KafkaConsumer` and `AsyncKafkaConsumer`. They cover `Subscribe(null)`,
`Subscribe(null, listener)`, `Subscribe(empty topic)`, `Assign(null)`, `Assign(null topic)` and,
async only, `Subscribe(null, async listener)`:
- `OnAClosedConsumer_ANullSubscribeOrAssign_ReportsTheClosedConsumer`: each throws
  `IllegalStateException` "This consumer has already been closed.";
- `WhileAnotherThreadIsInside_ANullSubscribeOrAssign_ReportsTheConcurrentAccess`: each throws
  `ConcurrentModificationException` "KafkaConsumer is not safe for multi-threaded access.".

Mutation check: going back to the bare `Snapshot*` calls fails all four cases.

## 3. Low: on the mocks, a null `Subscribe` / `Assign` argument still wins over the closed check

- **Where:**
  - `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs:953` `CheckAfterAcquire`: its mock branch
    runs `checks()` directly, before the closed check;
  - the six call sites it serves (`NativeConsumer.cs:568, 582, 694`; `NativeConsumer.Async.cs:96, 141,
    220`);
  - its comment at 950-952: "Java's MockConsumer has no check of its own, so the mocks keep their
    order".
- **Reference:**
  - Java's `MockConsumer.assign(Collection)` runs `ensureNotClosed()` first, then
    `new HashSet<>(partitions)`.
  - Java's private `subscribe(Collection, Optional)`, which `subscribe(topics)` and
    `subscribe(topics, listener)` call after the listener check, does the same before
    `new HashSet<>(topics)`.
  - So a null on a closed Java mock throws `IllegalStateException` ("This consumer has already been
    closed."), not the `NullPointerException`.
  - Python's mock agrees: `_mock_core.py` `_c_subscribe_topics` and `_c_assign` take the lock, run
    `self._lock.check()` and `self._ensure_not_closed()`, and only then `_require(...)`.
  - CLAUDE.md, Python Binding Conventions, Order: "The mocks keep Java's mocks' order."
- **Failure scenario:** on a closed `MockConsumer` or `AsyncMockConsumer`, `Subscribe(null)` and
  `Assign(null)` throw `ArgumentNullException` (from `SnapshotTopics` and `SnapshotPartitions`) where
  Java's and Python's mocks throw `IllegalStateException`. A test written against the mock to check
  closed handling catches the wrong class. The existing test
  `PublicSyncConsumerPreconditionTests.Subscribe_NullTopics_ThrownBeforeNativeCall_EvenWhenClosed`
  (line 145) pins the wrong order.
  - The order was already there before this phase and is on a misuse path, hence Low.
  - Finding 2's fix text told the Actor to keep the mocks as they were, on the mistaken premise that
    Java's mock checks differently. The comment at 950-952 repeats that premise.
- **Fix:**
  - In `CheckAfterAcquire`'s mock branch, run the checks inside the use after the closed check:
    `CheckInAUse(checks)`, as `CheckWhenOpen`'s mock branch does. This assumes the mock's
    `EnterUse` makes the closed check, as it does for `Unsubscribe`; otherwise call
    `ThrowIfClosed()` first. Keep the `ArgumentNullException` class, the .NET form of Java's NPE.
  - Change `Subscribe_NullTopics_ThrownBeforeNativeCall_EvenWhenClosed` to expect
    `IllegalStateException` with Java's closed message.
  - Add the closed-mock `Assign(null)` / `Subscribe(null, listener)` rows to
    `PublicConsumerCheckOrderTests`.
  - Correct the comment.

**Resolution (Actor 112):** `CheckAfterAcquire`'s mock branch now runs `CheckInAUse(checks)`, so the
mock's use makes the closed check before the argument checks, as Java's `ensureNotClosed()` does.
The null check keeps its `ArgumentNullException` class. Its comment in
`dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs` now says Java's MockConsumer checks the
arguments after `ensureNotClosed()`.

Tests:
- `PublicConsumerCheckOrderTests.OnAClosedConsumer_ANullSubscribeOrAssign_ReportsTheClosedConsumer`
  now runs on all four consumers (`AllConsumers`), including `MockConsumer` and `AsyncMockConsumer`.
  It asserts `IllegalStateException` "This consumer has already been closed.".
- `PublicSyncConsumerPreconditionTests.Subscribe_NullTopics_ThrownBeforeNativeCall_EvenWhenClosed` is
  renamed `Subscribe_NullTopics_WhenClosed_ReportsTheClosedConsumerFirst` and asserts the same.
- `ConsumerNegativePartitionGuardTests`: the two Assign tests also pinned the old order on a disposed
  mock. They now expect the closed `IllegalStateException` there. The async test now checks the
  guard on a live `AsyncMockConsumer` (it throws before a Task is returned, and nothing is assigned),
  and the class remarks no longer say Assign checks before the closed check.

Mutation check: going back to `return checks();` fails the two mock rows and the precondition test.
