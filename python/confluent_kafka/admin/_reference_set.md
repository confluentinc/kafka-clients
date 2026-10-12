# Admin Options / Result reference set

Hand-written from CLAUDE.md "## Python Binding Conventions" before the `cargo xtask` generator
exists. The generator must reproduce these files exactly (golden test). Each class covers the
patterns listed. "Coverage" below maps every one of the 45 Options and 44 Result classes to the
patterns it uses; the classes listed there as outside every pattern make the generator stop, and
are hand-written.
Tests: `python/test/unit/test_admin_reference_set.py`; typing: `_admin_reference_set_types` in
`python/test/unit/test_typing.py`.

## Files and layout

Every generated module is a pair: `<class>.py` (runtime; keyword defaults `_args.UNSET` or `None`)
and `<class>.pyi` (the typed surface, with `@overload` stubs and Java's starting values), as the
error generator already emits. mypy reads only the `.pyi`. Both start with the Confluent Apache 2.0
header; the `.pyi` has the one-line comment `# Stub for confluent_kafka.admin.<Class>.`. Internal
imports use the file path (`confluent_kafka.admin.abstract_options`), never the package. Types of
other modules a `.py` only annotates are imported under `TYPE_CHECKING`; one used at run time is
imported inside the function (`ListGroupsOptions.for_consumer_groups` imports `GroupType`).

`_futures.py` (private, not generated: written once) holds the client-kind marker and the future
helpers: `Sync` / `Async` (marker classes, no instances), `KindT = TypeVar("KindT", Sync, Async)`,
`AnyFuture[T]` (either future), `_then_apply`, `_all_of`, `_new_future`, `_new_future_like`
(Java's `new KafkaFutureImpl<>()` for a future completed from another's `whenComplete`),
`_when_complete`, `_completed_future`, `_complete`, `_complete_with_error` (a `CancelledError`
cancels), `_kind_of`.

`_internals.py` (private, written once) holds the Java internals results use: `_CoordinatorKey`
(`admin.internals.CoordinatorKey`, its `FindCoordinatorRequest.CoordinatorType` nested) and
`_sub_level_error` (`KafkaAdminClient.getSubLevelError`). Java's internal `Errors` enum is carried
as what `Errors.exception()` gives: the error the client builds from the FFI's per-key error
through the error registry (`_errors.from_ffi_error` / `class_for_ffi_id`), `None` for
`Errors.NONE`.

## Options classes

| Class | Patterns |
|---|---|
| `AbstractOptions` | Java `abstract`: `__init__` raises `TypeError("AbstractOptions is an abstract class; use one of its subclasses")` when the class is itself, else sets Java's field initializers; no keyword constructor. `timeoutMs()` / `timeoutMs(Integer)`: one method, two stubs, `-> Self`; the argument is nullable, so it defaults to `UNSET` and `timeout_ms=None` sets null. |
| `DeleteRecordsOptions` | Only the inherited `timeoutMs`: keyword constructor `(*, timeout_ms)`, nothing else. |
| `CreateTopicsOptions` | Re-declared `timeoutMs` (repeated with the class's own return type). Setter and getter with different names (`validate_only` / `should_validate_only`, `retry_on_quota_violation` / `should_retry_on_quota_violation`): the setter has a required keyword. |
| `DescribeTopicsOptions` | One-name setter + getter pairs with a non-nullable argument (`include_authorized_operations`, `partition_size_limit_per_response`): runtime default `None` = read; stubs `(self) -> T` and `(self, *, x: T) -> Class`. Starting values `False` and `2000`. Re-declared `timeoutMs`. |
| `DescribeFeaturesOptions` | A starting value with no value of the setter's type (`OptionalInt.empty()`, setter `int`): stub `node_id: int = ...`. The setter's `OptionalInt.of(x)` is `x`. |
| `ListGroupsOptions` | Setters storing a changed value (`null`/empty -> `Set.of()`, else a `Set.copyOf` copy; a `None` element raises `NullPointerError()`). Keywords named after fields, not setters (`group_states` -> `in_group_states`, `types` -> `with_types`, `protocol_types` -> `with_protocol_types`). Static factories -> `@staticmethod`. Java's internal `ConsumerProtocol.PROTOCOL_TYPE` -> a private module constant. Stubs show `= ()` for `Set.of()`. Translates `ListGroupsOptionsTest`. |
| `ListTopicsOptions` | `equals` / `hashCode` / `toString` -> `__eq__` (`type(other) is not type(self)` -> `NotImplemented`) / `__hash__` (tuple of Java's `Objects.hash` fields) / `__str__` (Java's text, values through `java_str`). Re-declared `timeoutMs`. |
| `CreateDelegationTokenOptions` | Several setters for one field: `maxLifetimeMs` and the deprecated `maxlifeTimeMs`; the keyword calls the one named like the field and the deprecated pair is not generated (listed in the module docstring). `owner`: a nullable argument of a one-name pair (`Optional.ofNullable` getter) -> `UNSET`. Starting values `-1`, `()` (`new LinkedList<>()`), `None`. Getter returns the stored `list` (Java returns its mutable field). |
| `RemoveMembersFromConsumerGroupOptions` | Not built with setters (a constructor takes arguments): Java's constructors only, `(*, members=None)`, `None` = the `()` form; Java's `IllegalArgumentException` message. Setter returning `void` -> returns `None`; nullable one-name pair -> `UNSET`. Translates `RemoveMembersFromConsumerGroupOptionsTest`. |
| `ListTransactionsOptions` | Set fields that Java's `hashCode` hashes: stored as `frozenset` (hashable), getters return a new `set`; `__hash__` hashes the frozensets. Keyword named after the field while the setter and its parameter have other names (`filtered_states` -> `filter_states(*, states)`). A nullable setter argument with its own name (`filter_on_transactional_id_pattern(*, pattern: str \| None)`, required). `toString` prints `timeoutMs`, `equals` / `hashCode` leave it out. `new HashSet<>(null)` is not checked. |
| `DescribeProducersOptions` | `OptionalInt` in `toString` (`OptionalInt[5]` / `OptionalInt.empty`); `equals` / `hashCode` over a field and `timeoutMs`; `broker_id: int = ...`. |
| `ListOffsetsOptions` | Constructor-only class with a Java-given enum default (`()` calls `this(IsolationLevel.READ_UNCOMMITTED)`): `isolation_level: IsolationLevel = IsolationLevel.READ_UNCOMMITTED`; a `final` field, so no setter. |

Keyword constructor (every setter-built class): parent fields first (`timeout_ms`), then the class's
own in Java's field order; runtime default `UNSET`; body = `super().__init__()`, Java's field
initializers, then for each given keyword (in dependency order; none of these classes has a
dependency, so keyword order) a call of its public setter by keyword. No `java_forms` and no
constructor `@overload`: any subset is a form.

## Result classes

Every result is `Generic[KindT]`, `__slots__` including `_kind`; methods returning a future (or a
map of futures) have one `@overload` per kind (`self: X[Sync]` -> `concurrent.futures.Future[...]`,
`self: X[Async]` -> `asyncio.Future[...]`). `__init__` of a non-public constructor raises
`TypeError("<Class> cannot be constructed directly; use the admin client's <method>()")`.

| Class | Patterns |
|---|---|
| `AlterConfigsResult` | Per-key futures map (`values()` returns the stored `dict`) + `all()` = `_all_of(values, kind)`. |
| `ListTopicsResult` | One future; `listings()` / `names()` = `_then_apply` (`Map::values` -> `list`, `Map::keySet` -> `set`). |
| `DescribeClusterResult` | Several independent futures, returned as stored; Javadoc "null otherwise" -> `set[AclOperation] \| None`. |
| `DeleteTopicsResult` | Constructor body with Java's checks and messages; package-private static factories -> private `@staticmethod`s `_of_topic_ids` / `_of_topic_names`; nullable map getters; conditional `all()`. Translates `DeleteTopicsResultTest`. |
| `CreateTopicsResult` | Futures carrying a private helper class `_TopicMetadataAndConfig` (decision log 6) with Java's two constructors under `java_forms(name="TopicMetadataAndConfig")` and `ensureSuccess`; package-private `UNKNOWN` -> `_UNKNOWN`; `values()` maps each future through `_then_apply(f, lambda v: None)`; per-topic getters on an unknown topic raise `NullPointerError()` (Java's NPE on `futures.get(topic).thenApply`). |
| `ListOffsetsResult` | Public nested value class `ListOffsetsResult.ListOffsetsResultInfo` (keyword constructor, getters, `__str__` rendering `Optional` as Java does); public constructor kept as `__init__(*, futures)` with the kind read from the futures, plus `_new`; `all()` = `_then_apply(_all_of(...), collect)`; `partition_result` raises Java's `IllegalArgumentException` message. |
| `DeleteConsumerGroupOffsetsResult` | A method building a new future completed from the stored future's `whenComplete` (`_new_future_like` + `_when_complete` + `_complete*`, early `return` inside the action); a private helper method (`maybeCompleteExceptionally` -> `_maybe_complete_exceptionally`); a map of Java's internal `Errors`; `KafkaAdminClient.getSubLevelError`; a synchronous `IllegalArgumentException` before the future. |
| `DescribeTransactionsResult` | A map keyed by Java's internal `CoordinatorKey` (`_CoordinatorKey`); lookup-or-`IllegalArgumentException`; `allOf` + `thenApply` collecting by `key.idValue`. |
| `ListTransactionsResult` | A future of a map of futures, both of the client's kind (stub: `Future[dict[int, Future[...]]]` per kind); nested `whenComplete` actions; `thenApply` of another public method. Java's `allByBrokerId()` never completes for an empty broker map (it completes only from a broker future); translated as is. |
| `DeleteConsumerGroupsResult` | A getter returning a copy of the stored map (`new HashMap<>` + `putAll` -> `dict(...)`). |

## Rule interpretations (for review)

1. `.py` + `.pyi` per generated module, so stubs can show Java starting values / `= ...` while the
   runtime default is `UNSET`.
2. `_new` takes a trailing keyword `kind` (`Sync` / `Async`) after Java's constructor parameters,
   stored as `_kind`; it is how "set by the client that returns it" is done, and `_all_of` needs it
   for an empty map. A result with a public constructor also gets `_new`, and its `__init__` reads
   the kind from the futures (none or `concurrent.futures.Future` -> `Sync`); its `.pyi` binds the
   kind with one `__init__` stub per future type.
3. `_new` copies a given map into a `dict` (the stubs promise `dict`).
4. A keyword constructor calls the public setter. For a one-name pair with a non-nullable argument,
   an explicit `None` therefore reads and leaves the field (the "not given" meaning of `None` for
   that method); for a setter with its own name it stores `None`.
5. A Java immutable set field (`Set.of` / `Set.copyOf`), or a set field Java's `hashCode` hashes,
   is stored as a `frozenset` and its getter returns a new `set`; any other Java mutable collection
   field is stored as a `list` / `set` and returned as is.
6. `_then_apply` / `_all_of` of done futures are done on return; `_all_of` fails with the first
   failed future in input order (as `CompletableFuture.allOf`), cancelled if that one was cancelled;
   an `asyncio.Future` is completed through `call_soon_threadsafe` from another thread; a new
   `asyncio.Future` with no source future needs a running loop (`IllegalStateError` otherwise).
7. Several setters for one field: the Options classes have no live case in Java 4.3.1
   (`CreateTopicsOptions.validate` is gone since 4.0; `CreateDelegationTokenOptions.maxlifeTimeMs` is
   deprecated, so not generated). The live case is `MockAdminClient.Builder` (`brokers(List<Node>)`
   calls `numBrokers(int)`, both writing `brokers`; the keyword calls the setter named like the
   field), which is also the golden case for setter dependency order; it is written
   by hand. A constructor with two keywords for one field (one `java_forms` form each) has no
   instance yet.
8. Null arguments: only Java's explicit checks are translated
   (`Objects.requireNonNull`, an `if (x == null) throw`). A `null` argument Java dereferences
   implicitly is not checked: Python raises what it naturally raises (`frozenset(None)` ->
   `TypeError`; `Set.copyOf` with a `null` element is not checked either). A `null` the class itself
   produces and then dereferences (a map lookup miss, `futures.get(topic).thenApply(...)` in
   `CreateTopicsResult`) is `NullPointerError()`.
9. `CreateDelegationTokenOptions.renewers` is not nullable, following the Signatures nullability
   text (starting value `new LinkedList<>()`, no null branch, no Javadoc; `KafkaAdminClient`
   iterates it): its runtime default is `None` (read) and the stub's argument is
   `Sequence[KafkaPrincipal]`.
10. `ListTransactionsResult.all_by_broker_id()` stays pending for an empty broker map, as Java's
    does.

## Coverage

Options (45): every class is covered.
- Only `timeoutMs` (`DeleteRecordsOptions`; re-declared: `CreateTopicsOptions`; `toString`:
  `ListTopicsOptions`): AbortTransaction, AlterConsumerGroupOffsets, AlterReplicaLogDirs,
  AlterUserScramCredentials, CreateAcls, DeleteAcls, DeleteConsumerGroupOffsets,
  DeleteConsumerGroups, DeleteRecords, DescribeAcls, DescribeClientQuotas, DescribeLogDirs,
  DescribeReplicaLogDirs, DescribeTransactions, DescribeUserScramCredentials, ElectLeaders,
  FenceProducers, ListConfigResources, ListPartitionReassignments, TerminateTransaction.
- One-name pairs (`DescribeTopicsOptions`) and differently named setter/getter (`CreateTopicsOptions`):
  AlterClientQuotas, AlterConfigs, AlterPartitionReassignments (setter parameter `allow`, keyword
  after the field), CreatePartitions (explicit no-argument constructor), CreateTopics, DeleteTopics,
  DescribeClassicGroups, DescribeCluster, DescribeConfigs, DescribeConsumerGroups, DescribeTopics,
  ExpireDelegationToken, ListConsumerGroupOffsets, ListTopics, RenewDelegationToken, UpdateFeatures.
- Nullable one-name pairs (`CreateDelegationTokenOptions.owner`): CreateDelegationToken,
  DescribeDelegationToken (`owners`).
- `= ...` (`DescribeFeaturesOptions`, `DescribeProducersOptions`): DescribeFeatures, DescribeProducers.
- Changed values / factories (`ListGroupsOptions`), hashed sets (`ListTransactionsOptions`):
  ListGroups, ListTransactions.
- Constructor-only (`RemoveMembersFromConsumerGroupOptions`, `ListOffsetsOptions`): RemoveMembers…,
  ListOffsets. Abstract: AbstractOptions.

Results (44):
- Stored futures / maps, `allOf` (`AlterConfigsResult`, `DescribeClusterResult`): AbortTransaction,
  AlterClientQuotas (public constructor, as `ListOffsetsResult`), AlterConfigs,
  AlterPartitionReassignments, AlterReplicaLogDirs, CreateAcls, CreateDelegationToken,
  CreatePartitions, DeleteRecords (public), DescribeAcls, DescribeClientQuotas (public),
  DescribeCluster, DescribeDelegationToken, DescribeFeatures, ExpireDelegationToken,
  ListPartitionReassignments, RenewDelegationToken, TerminateTransaction, UpdateFeatures.
- Copy getter + `allOf`/`thenApply` collect (`DeleteConsumerGroupsResult`, `ListOffsetsResult`):
  DeleteConsumerGroups, DescribeClassicGroups (public), DescribeConfigs, DescribeConsumerGroups
  (public), DescribeLogDirs.
- Lookup-or-`IllegalArgumentException` + collect, nested value class (`ListOffsetsResult`,
  `DescribeTransactionsResult`): ListOffsets, DescribeProducers (`PartitionProducerState`),
  DescribeTransactions.
- `thenApply` views (`ListTopicsResult`, `CreateTopicsResult`): ListTopics, CreateTopics.
- Exclusive maps, private static factories (`DeleteTopicsResult`): DeleteTopics.
- `whenComplete` into a new future (`DeleteConsumerGroupOffsetsResult`, `ListTransactionsResult`):
  DeleteConsumerGroupOffsets, ElectLeaders (`Map<TopicPartition, Optional<Throwable>>` ->
  `dict[TopicPartition, BaseException | None]`), ListConfigResources (relay), ListTransactions.

Outside every pattern (the generator stops; hand-written):
- `AlterConsumerGroupOffsetsResult`: `all()` throws `error.exception(message)` (an `Errors`
  exception with a built message) from inside `thenApply`.
- `AlterUserScramCredentialsResult`: constructor body `Collections.unmodifiableMap(
  Objects.requireNonNull(futures))`.
- `DeleteAclsResult`: `all()` / `getAclBindings` wrap a failure in `KafkaException(message, cause)`
  and throw `FilterResult.exception()`.
- `DescribeReplicaLogDirsResult`: its nested `ReplicaLogDirInfo` has only package-private
  constructors (an overloaded `_new` whose `()` form passes the internal constant
  `DescribeLogDirsResponse.INVALID_OFFSET_LAG`), `get*` getters, and a two-branch
  `StringBuilder` `toString`.
- `DescribeTopicsResult`: a private generic static `all(Map<T, …>)` and `allTopicNames()` /
  `allTopicIds()` returning `null` instead of a future.
- `DescribeUserScramCredentialsResult`: holds the generated `DescribeUserScramCredentialsResponseData`;
  `Errors.forCode(code).exception(message)`, `ResourceNotFoundException`.
- `FenceProducersResult`: `ProducerIdAndEpoch` and `findAndApply(…, KafkaFuture.BaseFunction)`.
- `ListConsumerGroupOffsetsResult`: constructor re-keys a `CoordinatorKey` map by stream; a
  size-checked `IllegalStateException` and `iterator().next()`.
- `ListGroupsResult`: constructor body splitting `Collection<Object>` by `instanceof Throwable` into
  three completed futures of unmodifiable lists.
- `RemoveMembersFromConsumerGroupResult`: `MemberIdentity` (generated `LeaveGroupRequestData`),
  package-private `MemberToRemove.toMemberIdentity()`, `KafkaException(message, cause)` wrapping.
