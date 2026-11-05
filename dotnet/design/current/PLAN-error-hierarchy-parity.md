# .NET error hierarchy parity

**Status:** APPROVED. D1–D5 are ruled (each marked **Ruled** below). D6–D24 are decided under the
guiding principle below (each marked **Decided**), because each has an answer that follows Java
and Python. No choice is open: the binding owner ruled the type a closed client throws (D20), and
D23's transaction question is deferred to the .NET transactions step, because this binding has no
transactional producer today. The binding owners agreed D1's rule change. The plan was revised after a review from
four angles (.NET, Java and the generator, Python, facts and phases), in two rounds. That review
corrected the counts, changed how D3, D4 and D5 are implemented (their outcomes are unchanged),
moved work between phases and added D18–D24. Each place where a ruled decision's mechanism changed
is marked **Implementation (revised)**.

**Guiding principle (binding owner):** align exactly with Java and Python. Java is the source
of truth, then the Rust core, then Python. Anything .NET keeps that neither has is a recorded
deviation.

**Mode:** A (.NET only) for every phase except Phase 2, which is Rust tooling under
`rust/xtask/` (see §5). Neither the Rust core nor the C header changes.

**Reference:** `ERROR-CATEGORIZATION.md` at the repo root holds the full per-language detail
and, in Appendix A, every class id with its Java and Python class. The C ABI the binding reads is
`rust/target/include/confluent_kafka_bindings.h` (BH; `ffi-marshalling.md` §0.1).

---

## 1 · Goal

A .NET user can classify a Kafka error the way a Java or Python user does, by its class:

```csharp
try { consumer.Commit(); }
catch (GroupAuthorizationException e) { Log(e.GroupId); }
catch (AuthorizationException)       { /* any authorization failure */ }
catch (RetriableException)           { /* retry */ }
```

A failed future's `Get()` throws an `ExecutionException` with the error as its
`InnerException`, as Java and Python do (D13, Phase 6), so there the class is matched on
`e.InnerException`.

**Done means:**

- Every Java exception class the C enum `kafka_common_Error_e` names has a C# class.
- Each class extends its Java parent and sits in the namespace its Java package gives it
  (`dotnet/CLAUDE.md` §2; the JDK classes: D19).
- Every core error arrives as the class of its id, with its extra fields filled in. Id −1 is two
  classes, told apart as Python tells them apart (D18).
- An error the binding raises for a Java built-in has the same class as the core's error for it
  (D20).
- Python already meets all of this (`python/confluent_kafka/_errors.py`); the Rust core keeps the
  same information as predicates.

## 2 · Where .NET is today

Verified against the code on this branch:

| Area | Today | Where |
|---|---|---|
| Error classes | `KafkaException`, plus `SerializationException` and `RecordDeserializationException`, both binding-made | `src/Confluent.Kafka/Common/`, `Common/Errors/` |
| Core error → exception | `FromFfiError` maps −2/−4, −3, −5 and −31 to .NET built-ins; every other id becomes a plain `KafkaException(code, message, isRetriable, inner)`. It never reads `ErrorIsApiError`, which `NativeMethods` declares | `Common/KafkaException.cs` |
| Second reader | `FromHandle` / `FromBorrowedHandle` skip the built-in table and the carried-exception check | used by `NativeMethods.Code`/`Message` (through `ThrowIfFailed`) and `GroupDescriptionMarshal.CopyOutErrors` |
| Extra fields | None read. No C payload view is declared in `NativeMethods` | `Internal/Interop/NativeMethods.cs` |
| `Code` | Stored by the internal constructors. The public constructors give 0, so `SerializationException`, `RecordDeserializationException` and user-built exceptions report 0. `WrapAtPoll` and `WrapAtClose` give −1 | `KafkaException.cs`, `KafkaExceptionCauseConstructorTests.cs` |
| Binding-made Kafka errors | `RequestTimedOut` (7) and `INVALID_GROUP_ID` (24) built as plain `KafkaException`; 15 "the admin result produced no …" invariant errors with `Code` 0 | `Internal/NativeConsumer.cs`, `Internal/Interop/*Marshal.cs` |
| Binding-made Java built-ins | The consumer's owner check throws `InvalidOperationException` with Java's `acquire()` message, where Python raises `ConcurrentModificationError`. A closed client throws `ObjectDisposedException` (Python: `IllegalStateError`). Java `IllegalArgumentException` checks throw `ArgumentException` / `ArgumentOutOfRangeException` (Python: `IllegalArgumentError`). A timed-out future `Get` throws `System.TimeoutException` (Java: `java.util.concurrent.TimeoutException`; Python: the root `TimeoutError`). There are 62 `new InvalidOperationException(` sites under `src/` | `NativeConsumer.cs`, `NativeProducer.cs`, `KafkaFutureImpl.cs`, `ListConsumerGroupOffsetsResult.cs` |
| Rethrow | `CreateTopicsResult.TopicMetadataAndConfig.EnsureSuccess` wraps the stored error in a new `KafkaException`, so the class is lost | `Admin/CreateTopicsResult.cs` |
| Admin error types | `ListGroupsResult.Errors()` is `KafkaFuture<IReadOnlyCollection<KafkaException>>` (Java: `Collection<Throwable>`); `ElectLeadersResult.Partitions()` is keyed to `KafkaException?` (Java: `Optional<Throwable>`), and its marshal fails the whole future on any other type; `DeleteAclsResult.FilterResult.Exception`, `LogDirDescription.Error` and the public `TopicMetadataAndConfig(KafkaException)` constructor take `KafkaException` where Java takes `ApiException` | `Admin/*.cs`, `Internal/Interop/ElectLeadersMarshal.cs` |
| Serializer failures | `Send` wraps every serializer exception in a `SerializationException`. Java wraps only a `ClassCastException`, and Python wraps nothing | `Internal/SerializedProducerRecord.cs` |
| Exceptions going into the core | A user's exception crosses as a `local_callback` error that carries the instance, and comes back as the same object. **Unchanged by this plan.** | `Internal/Interop/ManagedExceptionCarrier.cs` |
| Rules | ffi §A5/§B5 (decision 14) list "a per-code hierarchy now" as an anti-pattern. §A5 also says the ABI exposes only code, retriable and fatal; that is no longer true (173 predicates and the payload views exist, and there is no fatal export) | `.claude/rules/ffi-marshalling.md` |

The target class set comes from the Java sources and the C enum:

- The graph `cargo xtask generate-error-codes` builds has **175 classes**: 167 Kafka classes
  (including `KafkaException` and 5 abstract bases) and the 8 JDK classes of `JDK_BUILTINS`.
  `Exception`, `RuntimeException` and `Throwable` are terminal bases, outside the graph.
- The binding generates **165 Kafka classes**. `KafkaException` and `RecordDeserializationException`
  stay hand-written (D8). With D15 it also generates **all 8 JDK classes**.
- **13 classes carry extra fields**. 12 of them have a C payload view; only 5 have a C factory
  that builds a core error with its fields (Phase 4).
- **Packages of the 167:** `common.errors` (153), `clients.consumer` (6), `common` (2), and one
  each in `clients.producer`, `common.config`, `common.metrics`, `common.network`,
  `common.protocol.types` and `common.requests`.
- Some of these classes are not `@InterfaceAudience.Public` (for example
  `CorrelationIdMismatchException` in `common.requests`). They are public here because the core can
  raise them, as Python's Errors rule makes every class the C enum names public.

## 3 · Decisions

Each decision has a recommendation, and ⚠ marks the ones that change existing public behaviour.
The binding is pre-1.0, but every ⚠ item should still be listed in the release notes. D1–D5 are
ruled by the binding owner. D6–D24 are decided by the rule "take the option that follows Java,
then the Rust core, then Python"; where an option keeps both Java and Python, it is taken, and
where they disagree, Java's (and the core's) is taken. D20's closed-client choice, which had no
option that kept both Java and the .NET convention, was ruled by the binding owner.

**D1 · Lift the "flat for now" rule. (Blocker)**
Rewrite decision 14 and the places that state it:

- ffi §A5: the Decision, the "flat now" Rule text, the Note, the per-code-hierarchy
  anti-pattern, and the "only code/retriable/fatal" sentence (it is in §A5 only);
- ffi §B5: the Decision, the "flat now" Rule text (`Code` and `IsRetriable`), the Note, the
  per-code-hierarchy anti-pattern, the "Why" paragraph, the `IsRetriable` test, the wakeup
  bullet ("**Not** a subclass") and its anti-pattern, the `GroupMetadata()` bullet (D11), the
  "Concurrent use" bullet (D6, D20) and the "local codes map by the error table" bullet (D15);
- ffi §B1: the −31 `NotSupportedException` sentence, and the owner-check message type (D20);
- ffi §A7: "a retriable `KafkaException` with code 7 … not `System.TimeoutException`, which
  decision 14 keeps for -5" (D11, D15);
- `dotnet/CLAUDE.md` §2: "there is no root exception type", and the error-namespace bullet (D19);
- `dotnet/CLAUDE.md` §3: the `KafkaException` sketch, the `SerializationException : KafkaException
  { } // flat subclass` sketch, the `KafkaException` hierarchy row, the `wakeup()` row, the
  `ConcurrentModificationException` row and the `GroupMetadata()` comment (D11), and the
  "Java-contract fidelity" wording of the serializer wrap (D23);
- `dotnet/CLAUDE.md` §4: the Error granularity row, the `GroupMetadata()` text (D11), decision 17's
  text (D6, D20) and the "retriable `KafkaException`, code 7" text (D11);
- `dotnet/CLAUDE.md` §5: the §A5 index line ("flat `KafkaException` + preconditions");
- `dotnet/CLAUDE.md` §7.6: the gRPC errors text, and "the server calls only the public API" (D14);
- `dotnet/CLAUDE.md` §8.3: the Critic's lens;
- the passages that require `ArgumentOutOfRangeException` where Java throws
  `IllegalArgumentException` (D20): `dotnet/CLAUDE.md` §3's two "every TimeSpan: negative →
  ArgumentOutOfRange" sketch comments, §4's Disposal row ("A negative timeout throws
  `ArgumentOutOfRangeException` with Java's message", twice) and §4's `Seek` negative-offset guard;
  ffi §A5's Precondition bullet (a negative partition, a negative timeout) and its test, ffi §B5's
  Precondition bullet and ffi §C1's teardown text ("The timeout cannot be negative.").
- the passages that require `ObjectDisposedException` after close (D20): `dotnet/CLAUDE.md` §3's
  `IllegalStateException` row and §7.6's gRPC error arms; ffi §A1, §A2 (and its test list), §A5,
  §B1, §B2 (its teardown table and tests) and §B5.

The new rule: "every core error is the class of its id; the Java built-ins map to their
Java-named classes (D15), which derive from the .NET built-ins". *Recommendation:* agree it before
any code. It is the one item that needs the binding owners.
**Ruled:** approved as recommended. *(The passage list above was completed in review; the rule's
text follows D15's recommendation and is final once D15 is ruled.)*

**D2 · Generate the classes.**
Add a C# renderer to `rust/xtask/src/error_hierarchy.rs` beside the Python one. It reuses
`build_graph`, the `BRIDGE` table and the parsed Java classes, and `check-generated` covers
its output. The alternative, 165 hand-written classes, drifts from Java and the C enum.
*Recommendation:* generate. The generator is Rust, so Phase 2 belongs to the root personas
(`dotnet/CLAUDE.md` §8.1).
**Ruled:** approved as recommended. *Implementation (revised):* the renderer cannot reuse the
Python constructor `Effect`s (Phase 2).

**D3 · Constructors.**
C# has overloading, so each **public Java constructor becomes one C# constructor** with Java's
parameters, translated:

| Java | C# |
|---|---|
| `String` | `string?` |
| `Throwable` | `Exception? innerException` |
| `InterruptedException` | D24 |
| `Object` | `object?` |
| `int`, `long`, `double` | `int`, `long`, `double` |
| `TopicPartition` | `TopicPartition` |
| `Set<String>` | `IReadOnlyCollection<string>` |
| `Collection<TopicPartition>`, `Set<TopicPartition>` | `IReadOnlyCollection<TopicPartition>` |
| `Map<TopicPartition, Long>` | `IReadOnlyDictionary<TopicPartition, long>` |
| `Map<TopicPartition, OffsetAndMetadata>` | `IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>` |
| `KafkaMetric` | D7 |

The same table types the getters. Java public static factories become static methods
(`GroupAuthorizationException.ForGroupId`). Java's abstract bases become `abstract` classes with
`protected` constructors. Two constructors that map to the same C# signature stop generation with
an error.
*Recommendation:* as above. There is no `java_forms` table, because C# picks the overload itself.
**Ruled:** approved as recommended.

*Implementation (revised):*
- **The core-path constructor.** Each generated class gets one more constructor, which the core
  path (D7) calls: `internal X(CoreError _, string? message, Exception? innerException, …fields)`.
  It is `internal`, not `private protected`, so both the registry and the generated subclasses in
  the assembly can call it; code outside the assembly cannot.
  `CoreError` is an internal marker struct, so no Java constructor can produce the same signature;
  `(string?, Exception?)` alone would clash (CS0111) with the public constructor that Java's
  `(String, Throwable)` gives about 96 classes. It chains to its base's core-path constructor and
  passes up the fields its base declares, so `LogTruncationException` passes its offsets map to
  `OffsetOutOfRangeException`'s. The clash check covers it too.
- **Parameter names.** The `String` a constructor passes unchanged to `super(...)` as its message,
  directly or through `this(...)`, is named `message` whatever Java calls it
  (`InvalidRecordException(String s)`, `ProducerFencedException(String msg)`, the `("String", "s")`
  rows of `JDK_BUILTINS`), as Python's Errors rule names it. Other parameters keep Java's names, in
  camelCase. C# named arguments make the names part of the source API.
- **Singletons.** A Java `public static final` instance (`DisconnectException.INSTANCE`,
  `CoordinatorNotAvailableException.INSTANCE`) becomes `public static readonly X Instance`, built
  with the constructor Java names; a private Java constructor used for it is emitted `private`.
  Its doc says that an error the client throws is never this instance (`FromFfiError` builds a new
  object for every core error, where Java's internals raise the instance itself), so a caller
  compares by type (`ex is DisconnectException`); and that .NET records a stack trace on every
  `throw`, where Java's `ApiException` records none. Recorded in §7.
- **Getters.** Every Java getter becomes a property: `abstract` where Java's is abstract
  (`InvalidOffsetException.partitions()`), `override` where a base declares the same getter
  (decided from the hierarchy, not from `@Override`), and a key view for a `keySet()` body
  (`OffsetOutOfRangeException.partitions()`).

**D4 · ⚠ What `Code` means.**
`Code` becomes the class's id:

- each generated class with an id reports it;
- abstract bases report no id of their own; their concrete subclasses supply it;
- a bare `KafkaException`, whether built by the user, the binding (`WrapAtPoll`, the marshal
  invariants) or as a fallback for an unknown id, reports **−1**. That is the C id of a bare
  `KafkaException`, which it shares with `UnknownServerException` (D18); Python's
  `KafkaError._ffi_id` is also −1.

Today the public constructors give 0. *Recommendation:* −1, and update the `Code` doc comment,
which currently says "Kafka protocol error code".
**Ruled:** approved as recommended. Whether `Code` stays public at all is D14.

*Implementation (revised):* the id cannot travel up a constructor chain. A C# constructor can call
only its direct base's constructors, so `TopicExistsException : ApiException` would reach only
`ApiException`'s, which set −6, and the hand-written `RecordDeserializationException`, which calls
`SerializationException(message, cause)`, would report −15 instead of −27. So the id is a member
of the class, as Python's `_ffi_id` is a class attribute. The member that the generated classes
override must not be `Code` itself while D14 is open: an override cannot change the accessibility
of the member it overrides (CS0507), and only the Rust generator may change the generated files,
so a later `Code` → `internal` change in a Mode A phase would break every generated file. So:
- `KafkaException` declares `internal virtual int FfiId`, which returns −1 for a bare
  `KafkaException`;
- each generated class with an id overrides it with a constant (`internal override int FfiId`);
  abstract bases do not override;
- `RecordDeserializationException` overrides it with −27;
- `Code` is a non-virtual property that returns `FfiId`. D14 decides only `Code`'s own modifier,
  and the generated files are the same either way.

No constructor takes an id. Until Phase 3 the existing internal constructors keep a stored code
for the binding's own coded `KafkaException`s (`FfiId` returns it when set); Phase 3 moves those to
their classes (D11) and deletes the stored code. D14 lists the alternative mechanism.

**D5 · ⚠ Remove `IsRetriable`.**
Neither Java nor Python has it: the class is the category, so `catch (RetriableException)`
replaces `catch (KafkaException e) when (e.IsRetriable)`. A fallback `KafkaException` for an id
this binding doesn't know (a newer native library) loses the core's retriable flag; that matches
Python, whose fallback is a bare `KafkaError`.
**Ruled:** remove it (confirmed with the binding owner). Removal touches:
- `KafkaException`: the property, the `isRetriable` constructor parameters, and the
  `ErrorIsRetriableError` read in `FromFfiError`. The `NativeMethods` declaration goes if nothing
  else uses it. `ErrorIsApiError` stays (D18);
- `NativeConsumer.cs`, which passes the parameter;
- `CreateTopicsResult` (its doc and `EnsureSuccess`, which D10 rewrites anyway);
- the soak client's `ErrorIsRetriable`, which becomes `ex is RetriableException`, as Python's
  soak `error_is_retriable` checks `RetriableError`, and `SoakErrorClassificationTests`;
- 39 unit-test files, 3 `Confluent.Kafka.GrpcServer.UnitTests` files and the soak tests that
  construct or assert it;
- `dotnet/CLAUDE.md` §3 (the sketch and the hierarchy row), ffi §A5/§B5 and `DOTNET.md`.

The files under `design/history/` are records and stay unchanged.
*Implementation (revised):* the removal moves from Phase 1 to Phase 3. Before Phase 3 no class
exists for `ex is RetriableException` to match, so removing it earlier would make every error
non-retriable in the soak client.

**D6 · ⚠ The two JDK edge cases.**

- (a) **`CorrelationIdMismatchException` (−24).** Java's class extends `IllegalStateException`,
  and Python's `CorrelationIdMismatchError(IllegalStateError)` follows Java. Making it
  `: IllegalStateException` (D15), and so an `InvalidOperationException`, is Java-faithful, but it
  takes the class out of `catch (KafkaException)`, which catches it today.
- (b) **`ConcurrentModificationException` (−2)** is `InvalidOperationException` today, the same
  type as −4. `dotnet/CLAUDE.md` §3 already calls a subclass
  `ConcurrentModificationException : InvalidOperationException` non-breaking, and it lets a user
  tell −2 from −4, as Python can. That holds only if the binding's own owner check throws it too
  (D20).

*Recommendation:* (a) Java's place, under Java's `IllegalStateException` (D15), in
`Confluent.Kafka.Common.Requests`. (b) Add the subclass; its parent is settled by D15. The gRPC
server then sends −2 for (b) and −24 for (a), as Python does (Phase 3).
**Decided:** (a) and (b), as recommended: both are Java's and Python's classes.

**D7 · How the extra fields are read.**
A hand-written `internal` marshal, `Internal/Interop/ErrorPayloadMarshal.cs`, reads each C view
and calls the generated core-path constructor (D3). It mirrors Python's `py_KafkaError_payload` in
`python/_confluentkafka.c`. The view names in C aren't regular enough to derive reliably (compare
`kafka_common_Error_topic_authorization_error_err` with `kafka_common_Error_invalid_topic_err`),
and there are only 12 of them.

The generated registry maps each id to a factory delegate:
`(message, inner, error) => new X(default(CoreError), message, inner)` for a class without fields, or,
for a class with fields, a call to a fixed hand-written reader,
`ErrorPayloadMarshal.Read<Class>(message, inner, error)`. The generator never changes for Phase 4:
Phase 2 adds `ErrorPayloadMarshal` with readers that build the class from the message only, and
Phase 4 replaces their bodies with the view reads. `KafkaException` has no entry, as Python's
`_table()` skips `KafkaError`: −1 is D18's case. −27 has no entry either (D8).

A reader rebuilds a message through a Java constructor only where the core's message cannot be
Java's: `QuotaViolationException`, whose Java `getMessage()` is null, gets Java's text (D16).
Everywhere else the reader keeps the core's message. Where Java's class has several constructors
with different messages, the core's message already follows the constructor that Java's call site
uses. For example, `NoOffsetForPartitionException` is singular from `MockConsumer` and
`OffsetFetcherUtils` and plural from `SubscriptionState`. That reader builds only its set of
partitions. (Python's `_CORE_KWARGS` rebuilds the plural message, which differs from this.)

What C exposes:
- `CorrelationIdMismatchException`'s ids have no C accessor; they read −1, as Python's do.
- For `QuotaViolationException`, C exposes the value, the bound and the metric's full `MetricName`
  (name, group, description and tags, through `kafka_common_QuotaViolationError_metric_name_err`
  and the `kafka_common_MetricName_*_err` accessors), but no `KafkaMetric`. Python reads only the
  name and the group.
- No reader is needed for `RecordDeserializationException`: .NET passes `NULL` deserializers
  (ffi §B4), so the core never produces one for .NET (D8).

*Recommendation:* hand-write the readers. For `QuotaViolationException.Metric`, the options are:
- (a) Add `Confluent.Kafka.Common.Metrics.KafkaMetric : IMetric`, as Python has a `KafkaMetric`
  class, so the property and the public constructor keep Java's type. Java's `KafkaMetric`
  constructor takes a lock, a `MetricValueProvider`, a `MetricConfig` and a `Time`, none of which
  .NET has, so the class gets no public constructor; the core path builds it from the full
  `MetricName` and the value. Recorded in §7.
- (b) Type the property and the constructor parameter as `IMetric` (Java's `Metric`). Recorded in
  §7 as a narrowing of Java's type.

Recommend (a): it keeps Java's signatures, and the missing constructor is the smaller deviation.
Either way .NET reads the full `MetricName`, so `QuotaViolationException`'s text matches Java's
(D16). The −1 ids of `CorrelationIdMismatchException` are a limit of the core's C ABI, which
Python records as its own deviation; they go when the ABI exposes the ids.
**Decided:** hand-written readers, and (a) for `Metric`: it keeps Java's type, as Python keeps a
`KafkaMetric` class.

**D8 · What stays hand-written.**

- **`KafkaException`** holds `FromFfiError`, the wrap helpers, the base constructors and the
  virtual `Code` (D4).
- **`RecordDeserializationException`** has a .NET-specific shape (decision 22: `byte[]` buffers,
  `Headers`, the nested origin enum). Python generates it from Java's constructors; the shape is
  recorded in §7. Its only constructor takes the ten record fields, so it gets no core-path
  constructor and −27 gets no registry entry: a core −27, which .NET never receives (D7), would
  fall back to a bare `KafkaException`. The alternative, an internal core-path constructor with
  empty record fields, would invent field values the core never gave.
The D6 and D15 classes are generated, from the graph and the `JDK_BUILTINS` table.

The generator skips the hand-written ones through a `CSHARP_HAND_WRITTEN` list, and the hierarchy
test (Phase 3) still checks their parents and ids. `SerializationException` becomes generated; its
four public constructors match Java's, so its API is unchanged apart from `Code` (D4) and the
name of its cause parameter, `innerException` (D3), as on `KafkaException`: a call that names
the argument `cause:` no longer compiles.
*Recommendation:* as above.
**Decided:** as recommended. `RecordDeserializationException`'s shape is decision 22's, outside
this plan.

**D9 · ⚠ The admin surfaces typed `KafkaException`.**
- `ListGroupsResult.Errors()` becomes `KafkaFuture<IReadOnlyCollection<Exception>>` (Java's
  `Collection<Throwable>`). With D6, D15 and D18, a per-group error isn't always a
  `KafkaException`.
- `ElectLeadersResult.Partitions()` becomes `IReadOnlyDictionary<TopicPartition, Exception?>`
  (Java's `Optional<Throwable>`), and its marshal no longer fails the whole future for an error
  that is not a `KafkaException`.
- `DeleteAclsResult.FilterResult.Exception`, `LogDirDescription.Error` and the public
  `CreateTopicsResult.TopicMetadataAndConfig(KafkaException)` constructor and field take
  `ApiException`, as Java's and Python's do. The marshal that builds them casts the class
  `FromFfiError` returns; a core error that is not an `ApiException`, which Java's broker path
  cannot produce, fails the result's future with that error.

*Recommendation:* all three, with the async peers matched. The gRPC server's two `Errors()` sites
change in the same phase (Phase 3).
**Decided:** all three: each takes Java's type, as Python's does.

**D10 · Rethrow the original exception in `EnsureSuccess`.**
Java's `TopicMetadataAndConfig.ensureSuccess()` throws the stored exception itself. Rethrow that
instance with `ExceptionDispatchInfo.Capture(_exception).Throw()`, which keeps its class and stack
trace, instead of wrapping it. The stored exception is an `ApiException` (D9).
*Recommendation:* do it.
**Decided:** do it, as Java and Python.

**D11 · ⚠ Give binding-made Kafka errors their class.**

- `NativeConsumer`'s `RequestTimedOut` becomes `Confluent.Kafka.Common.Errors.TimeoutException`.
- The no-`group.id` error becomes `InvalidGroupIdException`, with Java's message unchanged.
- The in-callback code-7 timeout of ffi §A7 becomes `Confluent.Kafka.Common.Errors.TimeoutException`,
  as Python's `KafkaTimeoutError`.
- The 15 "the admin result produced no …" invariant errors have no Java counterpart, so this is a
  Python-parity question. Python raises no Kafka class for them:
  - where a single value is missing, Python substitutes one: `None` for a NULL delegation token,
    feature metadata or transaction description, `[]` for producer states, `""` for a NULL
    transactional id (`python/_confluentkafka.c`: `delegation_token_to_py`,
    `conv_feature_metadata`, `conv_transaction_description`, `conv_partition_producer_state`, and
    `transaction_listing_to_py` for the transactional id);
  - where a whole result or future is missing, Python raises the built-in `RuntimeError`, which
    has no `_ffi_id`, so its gRPC server sends it through its fallback: −4 with
    "python server: RuntimeError: …".

  The options are (a) follow Python per site: return `null` or an empty value where Python
  substitutes one, and throw a plain `InvalidOperationException` where Python raises
  `RuntimeError`, which the gRPC server sends through its own fallback, −4 with
  "dotnet server: InvalidOperationException: …" (Phase 3 lists the server's arms); or (b) keep the
  bare `KafkaException` (−1) and list it in §7.

*Recommendation:* the first three as above, and (a) for the invariant errors. Phase 3 checks each
of the 15 sites against the Python function that handles the same gap. ⚠ Under (a) some of these
calls stop throwing, and the rest are no longer caught by `catch (KafkaException)`.
**Decided:** the first three, and (a) for the invariant errors: Java's classes where Java has
them, Python's handling where only Python has one.

**D12 · The soak client's checks.**
There is no Java soak client, so Python's (`python/soak/soakclient.py`) is the reference. It
reads each error's id (`error_code` reads `type(ex)._ffi_id`) and classifies with code tables:
auth `{29, 30, 31, 33, 34, 58}`, topic-exists 36, coordinator `{14, 15, 16}` and disconnect
`{5, 6, 7, 8, 13}`. Its retriable check is `isinstance(ex, RetriableError)`.
*Recommendation:* keep .NET's soak identical to Python's: the same code tables (including
`UnknownTopicOrPartitionCode` 3, which .NET has today), read from `Code` (internal, D14), and
`ex is RetriableException` for retriability (D5). `ErrorCode` reads the id of every class that has
one, the D15 classes and `CorrelationIdMismatchException` included, through the generated id
lookup (Phase 2), as Python's reads any class's `_ffi_id`. No change to the Python soak client is
needed.
**Decided:** identical to Python's soak client.

**D13 · ⚠ Wrap a failed future's `Get()` in `ExecutionException`.**
Java's `Future.get()` and `KafkaFuture.get()` throw `ExecutionException` with the error as its
cause, and Python raises `ExecutionError` with the error as `__cause__`. .NET rethrows the error
itself, a recorded deviation (decision 18). Align with both:
- `Get()`, `Get(TimeSpan)`, `Get(CancellationToken)` and `GetNow` of `KafkaFuture<T>` and
  `KafkaFuture`, and the `Task`s of the async `Get` forms, fail with an `ExecutionException`.
  Its `InnerException` is the stored error, and its message is Java's `Throwable.toString()` of
  the error, built by the D21 helper (not .NET's `Exception.ToString()`).
- A timeout throws the Java-named `TimeoutException` of `java.util.concurrent` (D15), as Java's
  `get(long, TimeUnit)` throws it and Python's timed-out `get` raises the root `TimeoutError`. It
  is a `System.TimeoutException`, so existing `catch` blocks keep working. Its message is Java's:
  none for every future but a send's, as `CompletableFuture` throws it; and for a send's future
  (`Send`'s `KafkaFuture<RecordMetadata>` and `AsyncKafkaFuture<RecordMetadata>`)
  "Timeout after waiting for <ms> ms.", as Java's `FutureRecordMetadata.get(long, TimeUnit)`.
  Python gives a send's timeout no message; that gap is Python's, and .NET follows Java.
- Unchanged, as in Java: a cancelled future still throws `OperationCanceledException`; and
  `WhenComplete`, `ThenApply` dependants, the delivery callback and the commit callbacks receive
  the raw error.
- `ToCompletionStage()` on the async futures returns a `Task` that faults with the raw error, as
  Java's `CompletionStage` and Python's `to_completion_stage()` do. It is no longer the same
  `Task` as `Get()`.
- Internal waits (the admin `Result` classes' `GetNow` after `AllOf`) keep the raw error.
- `ExecutionException` is a JDK class with no .NET equivalent (`AggregateException` holds many
  errors). It becomes a Java-named class, `ExecutionException : Exception`, placed as D19 places
  the other JDK classes.

*Recommendation:* do it, as Phase 6. It breaks every caller that catches the error directly after
`Get()`.
**Decided:** do it, as Phase 6 (approved by the binding owner when it was added). It removes
decision 18's recorded deviation.

**D14 · ⚠ Make `Code` non-public.**
Java exceptions carry no code, and Python keeps its id private (`_ffi_id`) and classifies only by
class. A public `Code` is a deviation. Make it `internal`:
- `InternalsVisibleTo` for the assemblies `Confluent.Kafka.GrpcServer`, `SoakClient` and
  `SoakClient.Tests` (their `AssemblyName`s; `Confluent.Kafka.Soak` is only the soak client's
  root namespace), each with the shared public key, beside today's two unit-test assemblies, so the
  gRPC server reads the id as Python's server reads `_ffi_id`, and the soak client as Python's soak
  does (D12);
- the soak client keeps its code tables (D12); the soak tests' `Code` assertions keep compiling;
- `dotnet/CLAUDE.md` §7.6's "the server calls only the public API" gains this one exception.

**How `Code` and the generated overrides fit (part of this choice).** An override cannot change
the accessibility of what it overrides (CS0507), and the generated files change only through the
Rust generator, so `Code`'s accessibility and the member the generated classes override must be
settled before Phase 2. The options are:
- (i) the generated classes override a separate `internal virtual int FfiId`, and `Code` is a
  non-virtual property returning it (D4). The generated files do not depend on this decision, and
  making `Code` internal is a one-line change in Phase 5;
- (ii) `Code` itself is `virtual` and overridden, with its final accessibility set in Phase 1 (with
  the `InternalsVisibleTo` entries there if D14 is accepted), so the generator emits
  `public override` or `internal override` from the start.

*Recommendation:* make `Code` internal, ruled with the binding owner, through (i). If kept public,
record it as a deviation; (i) still works unchanged.
**Decided:** internal, through (i): Java has no code on its exceptions and Python keeps its id
private. (i) also keeps the generated files independent of this choice.

**D15 · ⚠ Java-named classes for the JDK exceptions.**
Python gives each Java built-in exception the binding raises its own Java-named root class over
Python's built-in: `IllegalStateError(RuntimeError)`, `IllegalArgumentError(ValueError,
RuntimeError)`, `ConcurrentModificationError(RuntimeError)`, `UnsupportedOperationError
(NotImplementedError)`, `TimeoutError(builtins.TimeoutError)` and `ExecutionError(Exception)`, plus
`NoSuchElementError(RuntimeError)` and `IndexOutOfBoundsError(IndexError, RuntimeError)`. .NET maps
them straight to its built-ins, with no Java-named class. Align with Python:

| Java (C id) | .NET class | Base |
|---|---|---|
| `IllegalStateException` (−4) | `IllegalStateException` | `InvalidOperationException` |
| `IllegalArgumentException` (−3) | `IllegalArgumentException` | `ArgumentException` |
| `ConcurrentModificationException` (−2) | `ConcurrentModificationException` | `InvalidOperationException` |
| `UnsupportedOperationException` (−31) | `UnsupportedOperationException` | `NotSupportedException` |
| `java.util.concurrent.TimeoutException` (−5) | `TimeoutException` | `System.TimeoutException` |
| `ExecutionException` (none) | `ExecutionException` | `Exception` (D13) |

Each base is the .NET built-in .NET maps the class to today, so existing `catch` blocks keep
working. Java's parent is `RuntimeException` for four of them and `Exception` for
`TimeoutException`; .NET has no `RuntimeException` and no multiple inheritance, so the .NET base
stands in for Java's chain (§7). `CorrelationIdMismatchException` extends `IllegalStateException`
(D6).

`NoSuchElementException` and `IndexOutOfBoundsException` have no C id, and nothing in .NET throws
them: Java's one public `NoSuchElementException` is `TimestampType.forName`, which .NET's
`TimestampType` enum does not have, and Python raises the other only from its pure-Python admin
mock. The options are (a) skip them, recorded in §7, and add each when a translated member throws
it; or (b) generate all eight, with bases `InvalidOperationException` and
`ArgumentOutOfRangeException`.

*Recommendation:* add the six, generated from `JDK_BUILTINS` with a C# base column, placed as D19
decides, and (a) for the other two. The core path then builds them in place of the plain built-ins
(Phase 3), the binding's own throws use them too (D20), and the gRPC server sends their ids
(Phase 3).
**Decided:** the six, and (b) for the other two, so .NET has Python's eight JDK classes:
`NoSuchElementException : InvalidOperationException` in `Confluent.Kafka.Java.Util` and
`IndexOutOfBoundsException : ArgumentOutOfRangeException` in `Confluent.Kafka.Java.Lang`
(`System.IndexOutOfRangeException` is sealed). They have no C id, as in Python, so the gRPC server
sends them through its fallback, as Python's does. This removes the §7 entry for them.

**D16 · `toString()` overrides.**
Java's `QuotaViolationException` passes no message (`getMessage()` is `null`) and overrides
`toString()`. A C# `ToString()` override is the wrong form of it: .NET's `Exception.ToString()`
is what loggers print, with the inner exceptions and the stack trace, and the binding's wrapping
messages are built from `Message`, not from `ToString()`. Python translates the override into
`__str__`, its form of the message: the override's text after the class name.
*Recommendation:* the Phase 2 renderer does what Python does:
- the class's `Message` is the override's text after the class name (Python's choice; Java's
  `getMessage()` is `null`, recorded in §7);
- the class overrides the internal Java-`toString()` of D21, so a wrapping message
  (`ExecutionException`, a `(Throwable cause)` constructor, `WrapAtPoll`) carries Java's full text;
- it does not override `ToString()`.

**Decided:** as recommended, as Python.

**D17 · Serialization (recorded deviation).**
Java exceptions are `Serializable`, and Python supports `pickle` and `copy` for its errors. .NET
adds nothing: binary serialization of exceptions is obsolete in .NET (SYSLIB0051), and none of
today's exceptions support it.
*Recommendation:* record it as a deviation and don't implement it.
**Decided:** as recommended. There is no option that follows Java and Python: .NET has removed
the mechanism.

**D18 · ⚠ Id −1: `KafkaException` or `UnknownServerException`.**
The C enum has no id of its own for a bare `KafkaException`: the core reports one as
`UNKNOWN_SERVER_ERROR` (−1, BH:208), the id of `UnknownServerException`, and the generator gives
both classes −1. About 56 places in the core build such an error ("The producer closed
forcefully", "Producer closed while send in progress", the in-callback `Flush` error). An
id-keyed registry would turn all of them into `UnknownServerException`, an `ApiException`, so
`catch (ApiException)` would catch errors Java throws as a plain `KafkaException`. Python tells
the two apart with the core's `is_api_error` predicate: −1 with `is_api_error` false is a bare
`KafkaError`, on the owned and on the borrowed path (`python/confluent_kafka/_errors.py`,
`_build`). The options are:
- (a) as Python: read `kafka_common_Error_is_api_error_err` for −1; false builds a bare
  `KafkaException`, true builds `UnknownServerException`;
- (b) −1 always builds a bare `KafkaException`, so `UnknownServerException` never arrives from the
  core;
- (c) −1 always builds `UnknownServerException`.

*Recommendation:* (a). It is Python's rule, it keeps every core error Java throws as
`KafkaException` out of `catch (ApiException)`, and `NativeMethods` already declares
`ErrorIsApiError`. ⚠ A broker's `UNKNOWN_SERVER_ERROR` then arrives as `UnknownServerException`.
**Decided:** (a), Python's rule, which gives each error the class Java throws.

**D19 · Where the JDK classes live.**
Python puts them at its root package. In .NET the root `Confluent.Kafka` is not a neutral place:
C# looks a simple name up in each enclosing namespace (`Confluent.Kafka.X`, then
`Confluent.Kafka`) before the compilation unit's `using` lines. So a root
`Confluent.Kafka.TimeoutException` would silently replace `System.TimeoutException` in every file
of the library, the tests, the gRPC server, soak and perf, with no diagnostic: 40 files use the
simple name today (`KafkaFutureImpl.cs:155,307,391`, `KafkaException.cs:177`, 25 exact-type test
assertions in 11 files). Throw sites and `Assert.Throws<TimeoutException>` would rebind together,
so the tests would not notice. Generated code is caught too: `BufferExhaustedException` in
`Confluent.Kafka.Producer` would get the root class as its base even with
`using Confluent.Kafka.Common.Errors;`. Root placement also ends `dotnet/CLAUDE.md` §2's rule that
no public type is reachable through the enclosing `Confluent.Kafka` namespace. The options are:
- (a) Java-package namespaces, derived as every other type's is (§2): `java.lang` →
  `Confluent.Kafka.Java.Lang`, `java.util` → `Confluent.Kafka.Java.Util`, `java.util.concurrent` →
  `Confluent.Kafka.Java.Util.Concurrent`. None encloses binding code, so nothing rebinds; a clash
  with `System.TimeoutException` becomes a visible CS0104, which §2's alias rule handles; §2's rule
  and `NoPublicType_IsInTheRootNamespace` still hold. A deviation from Python's root placement,
  recorded in §7.
- (b) The root, as Python. Phase 2 then first writes every `System.TimeoutException` the binding
  means fully qualified (about 85 sites), adds a check that keeps it so (a test or a
  banned-symbol analyzer), amends §2 and the layout test, and §6 describes the silent shadowing.

*Recommendation:* (a). Either way the generator writes every base type and every JDK type fully
qualified (`global::`), with a test that checks `BufferExhaustedException`'s base.
**Decided:** (a). It names the namespaces after Java's packages, as every other .NET type is named
(§2), and root placement would break the binding silently; the placement differs from Python's,
recorded in §7.

**D20 · ⚠ Binding-made throws that stand for a Java built-in.**
D15 converts the core's errors only. The binding also throws .NET built-ins where Java and Python
throw a Java built-in, so one condition would have two types. The worst case is −2: the
consumer's owner check refuses another thread before the native call with Java's `acquire()`
message as a plain `InvalidOperationException`, while the core's flag reports the same condition
as −2 (ffi §B1, "checked twice"). Python raises `ConcurrentModificationError` for both. Without
this decision `catch (ConcurrentModificationException)` misses the common path, D6(b)'s aim fails,
and the gRPC server sends −4 where Python sends −2. The sites:
Each site is classified by what the Java method it translates throws, not by its .NET type:
- the owner check (`NativeConsumer.cs`, six sites) → `ConcurrentModificationException`;
- a check where Java throws `IllegalStateException` or `IllegalArgumentException` → the D15 class
  of the Java exception, with Java's message. Today several of these throw `ArgumentNullException`
  or `ArgumentOutOfRangeException`, not `ArgumentException`. Examples:
  `ListConsumerGroupOffsetsResult.PartitionsToOffsetAndMetadata`; `ProducerRecord`'s topic and
  partition checks (Java: "Topic cannot be null." and "Invalid partition: %d. Partition number
  should always be non-negative or null."; .NET today: `ArgumentNullException` and
  `ArgumentOutOfRangeException` "Partition must not be negative."; Python raises
  `IllegalArgumentError` with Java's text); the consumer's `Subscribe` and `Assign` checks of a
  null collection, a null or blank topic and a null listener (Java: "Topic collection to subscribe
  to cannot be null", "… cannot contain null or empty topic", "Topic partitions collection to
  assign to cannot be null", "RebalanceListener cannot be null"; .NET today splits them between
  `ArgumentNullException`, a plain `ArgumentException` and the core's −3); the negative-timeout
  checks ("The timeout cannot be negative.", and Java's `Timer` "Invalid negative timeout …");
  `Seek`'s negative offset;
- a closed client: `ObjectDisposedException` today, Python's `IllegalStateError` → the D15
  `IllegalStateException` with the client's own Java message (ruled below);
- a .NET-only precondition: a site where Java throws `NullPointerException` (Python raises
  `TypeError`) or nothing at all.

*Recommendation:* the first two use the D15 classes. Phase 3 audits the 62
`new InvalidOperationException(` sites and every `ArgumentException`, `ArgumentNullException` and
`ArgumentOutOfRangeException` throw, each against the Java method it translates. Keep the
.NET-only preconditions of the last bullet as .NET idiom, recorded in §7. A test asserts that an
owner-check rejection and a core −2 have the same type.
**Decided:** the first two bullets and the last one, as recommended. The last one follows Python's
move, which maps Java's `NullPointerException` to its own `TypeError`.

**Ruled (binding owner): the closed client.** A call on a closed client throws the D15
`IllegalStateException` with the client's own Java message, as Java's and Python's do (Python's
`IllegalStateError` is its own class over `RuntimeError`, raised by its producer, consumer and
admin closed checks). For the admin client this is Python's form, not Java's: Java's
`KafkaAdminClient` returns the Result and fails its futures with that exception, but the native
handle is gone after `Close`, so the call has no Result to return; Python records the same
deviation (§7):
- the producer: "Cannot perform operation after producer has been closed";
- the consumer: "This consumer has already been closed.";
- the admin client: "Cannot accept new calls when AdminClient is closing."

The Rust core's own closed check (`KafkaProducer::ensure_not_closed`) raises the same error, so a
call that races `Close` and reaches the core gets the same type as one the binding refuses. It is
still an `InvalidOperationException`, so `catch (InvalidOperationException)` keeps working; `catch
(ObjectDisposedException)` no longer catches a closed client. Phase 3 changes the seven throw sites
in `NativeProducer`, `NativeConsumer` and `NativeAdminClient`, converts the
`ObjectDisposedException` that a released `SafeHandle` throws on the race path wherever it can
reach the caller, and updates the doc comments and the tests that assert `ObjectDisposedException`;
the internal catches of the runtime's own `ObjectDisposedException` (`NativeFutureSource.TryAddRef`)
stay. `ConsumerHandle`'s own check keeps `ObjectDisposedException`: it guards the handle's own
`Dispose`, a .NET object with no Java counterpart (Python's handle is not public), so the .NET
convention applies; a call through a live handle on a closed consumer gets the core's error, the
same `IllegalStateException`. D1 lists the rule passages. Calling `Close` twice stays harmless, and
`Wakeup` after close still does nothing.

**D21 · ⚠ Java's text.**
Python's renderer formats every message the Java way (`java_str`, `cause_message`), and the plan
needs the same, through two internal helpers:
- `JavaText.ValueOf(object?)`, Java's `String.valueOf`: `null` → `"null"`, a non-null `string`
  unchanged, `bool` → `"true"` / `"false"`, an integer in the invariant culture, `double` /
  `float` → Java's `Double.toString` / `Float.toString` in the invariant culture, a collection →
  `[e1, e2]` and a map → `{k=v}` with each element, key and value formatted through `ValueOf`
  itself (as Java's `AbstractCollection.toString` and `AbstractMap.toString` use
  `String.valueOf`), anything else its `ToString()` (`TopicPartition`, `MetricName` and
  `OffsetAndMetadata` already print Java's form);
- `JavaText.ToString(Exception)`, Java's `Throwable.toString()`: the class's override where it has
  one (D16); otherwise the type's name, plus `": " + message` only when Java's message is
  non-null. It replaces `CauseMessage`, and builds the message of `ExecutionException(cause)`
  (D13), of every generated `(Throwable cause)` constructor, of `WrapAtPoll` and of the
  local-callback wrap, and the exception part of `KafkaFuture.ToString()`, as Java's
  `KafkaFutureImpl.toString` prints the exception's `toString()`.

The choice is which name `ToString(Exception)` prints: (a) the .NET type's full name
(`Confluent.Kafka.Common.Errors.TopicExistsException`), as Python prints its Python class path;
or (b) Java's class name (`org.apache.kafka.common.errors.TopicExistsException`), through the
generated registry. *Recommendation:* (a), recorded in §7 as Python records its own.
**Decided:** (a), the same choice Python made for its class path.
⚠ Phase 1 switches `KafkaException(Exception cause)`, `WrapAtPoll` and the local-callback wrap
from `CauseMessage` to `JavaText.ToString`, so for a cause whose Java message is `null` the text
changes from "<type>: Exception of type '…' was thrown." to "<type>".

**D22 · ⚠ Java's `null` message.**
Java's `getMessage()` can be `null` (a no-argument constructor, a `null` message argument, a
timed-out `get`). Python's `str(e)` is then `""`, and its gRPC server sends `""`. A class that
overrides `toString()` (`QuotaViolationException`) is the exception: its `Message` is D16's text,
in Python as here. .NET's
`Message` falls back to "Exception of type '…' was thrown.", which the .NET gRPC server sends. The
options are: (a) `KafkaException` and the JDK classes keep Java's message in an internal field
(`null` included), `Message` returns `""` for a Java `null`, and `JavaText.ToString` reads the
field; (b) keep .NET's default text and record it in §7, the gRPC server sending `""` for a Java
`null`; (c) keep .NET's default text everywhere and record it.
*Recommendation:* (a), as Python's `str(e)`. The internal field is needed for D21 anyway. ⚠ Under
(a) `new KafkaException()`, `new KafkaException((Exception?)null)` and
`new SerializationException()` return `""` for `Message`, where today they return .NET's default
text (asserted by `KafkaExceptionCauseConstructorTests.NullCause_MessageIsTheDefaultMessage`).
**Decided:** (a), as Python's `str(e)`.

**D23 · ⚠ The serializer wrap.**
`Send` wraps every exception a serializer throws in a `SerializationException`. Java's
`KafkaProducer.doSend` does this with a serializer's exception:
- a `ClassCastException` is wrapped in a `SerializationException` ("Can't convert key of class …
  to class … specified in key.serializer");
- an `ApiException` is caught by `doSend`'s outer `catch (ApiException e)` and not thrown: the
  callback gets placeholder metadata and the error, `transactionManager.maybeTransitionToErrorState`
  runs, and `send` returns a failed future;
- any other exception propagates unchanged.

Python raises every serializer error from `send()`, unwrapped. With a class hierarchy, Java and
Python users catch a serializer's failure by its own class, and .NET users cannot.
`dotnet/CLAUDE.md` §3 calls the wrap "Java-contract fidelity", which is not so. The options are:
- (a) rethrow the serializer's exception unchanged, and wrap only an `InvalidCastException`
  (.NET's `ClassCastException`) in a `SerializationException`;
- (b) keep the wrap and record it in §7.

Under (a), a serializer's `ApiException` is a second choice: (a1) throw it from `Send`, as Python
does; or (a2) deliver it to the returned future and the callback, as Java does. Under either, the
core never sees the error, so it cannot run Java's `maybeTransitionToErrorState` (the deferred
note below).

The wrapped message cannot be Java's verbatim, because .NET has no `key.serializer` config entry
(serializers are constructor instances). The .NET text is "Can't convert key of class <the key's
runtime type> to class <the serializer's runtime type> specified as the key serializer" (and the
same for the value), the key's type printed as `null` for a null key. .NET's generics are reified,
so Java's usual trigger (an erased-generic cast that fails at the serializer) cannot happen; the
wrap catches only an `InvalidCastException` a serializer throws itself, which Java's `catch` wraps
too.

*Recommendation:* (a), in Phase 5, with the §3 wording corrected in Phase 0. (a1) keeps `Send`'s
failure on the calling thread, as today and as Python; (a2) is Java's and changes where the error
surfaces. The consumer's deserializer wrap is unchanged: Java's `RecordDeserializationException`
is that wrap.

The Rust core does what Java does: `KafkaProducer::do_send` wraps nothing, and sends a
serializer's `ApiException` through `handle_api_error` (the callback with placeholder metadata, a
failed future, and `maybe_transition_to_error_state`), returning any other error as is
(`rust/src/producer/kafka_producer.rs`, `do_send`, `handle_api_error`).

**Decided:** (a) with (a2). Java and the Rust core agree on (a2) and only Python differs, so the
priority order picks it. The binding reproduces `handle_api_error`'s callback and failed future.
The difference from Python's `send()` is recorded in §7.

**Deferred to the .NET transactions step: the transaction state.** Java's
`maybeTransitionToErrorState` moves a transactional producer to its abortable error state. This
binding has no transactional producer today: the transaction operations are a later step
(`STATUS.md`), so the question does not arise in this plan. When they return, a serializer's
`ApiException` inside a transaction would leave the producer able to commit, where Java's and the
Rust core's must abort. Python, which has transactions and serializes before the core, has that
gap today. That step chooses one of three options: accept the gap, as Python does; run the
serializers inside the core through the C serializer interface
(`kafka_common_serialization_Serializer_new_async`), so the core's `do_send` handles the error as
Java does; or add a core call that marks the transaction failed. Nothing in this plan depends on
that choice. For a producer that is not transactional, Java changes state only for five fatal
classes (`ClusterAuthorizationException`, `TransactionalIdAuthorizationException`,
`ProducerFencedException`, `UnsupportedVersionException`, `InvalidPidMappingException`), which a
serializer has no reason to throw; §7 records it.

**D24 · `InterruptException`'s `InterruptedException`.**
Java's `InterruptException(InterruptedException)`, `(String, InterruptedException)` and `(String)`
take a JDK class .NET does not have, and every one of them re-interrupts the thread
(`Thread.currentThread().interrupt()`). Python types the cause as `BaseException`, leaves it empty
for `new InterruptedException()`, and does not re-interrupt. The options are (a) as Python: the
parameter is `Exception? innerException`, `new InterruptedException()` becomes `null`, and no
re-interrupt; or (b) `System.Threading.ThreadInterruptedException`, with a new instance for
`new InterruptedException()`.
*Recommendation:* (a), with the missing re-interrupt recorded in §7. .NET's `Thread.Interrupt()`
does leave an interrupt pending, which the thread's next `Sleep`, `Join` or `Monitor.Wait` throws
as `ThreadInterruptedException`; but this error reaches .NET as a core error (−12), not from an
interrupt of the .NET thread, so re-interrupting would only make an unrelated later wait throw.
**Decided:** (a), as Python.

## 4 · Out of scope

- Any change to the Rust core, the C header or the Python binding. The one Rust change is the
  xtask generator (Phase 2).
- `IsFatal`. The C ABI has no fatal predicate.
- C# predicates (`IsAuthorizationError()` and the like). The class is the category, as in Python.
- The share consumer's errors (consumer-threading §20).

## 5 · Phases

Each .NET phase ends green on `make verify-dotnet` (`dotnet/CLAUDE.md` §7.5), which runs the
unit, integration and soak tests and the `__grpc_dotnet` / `__grpc_dotnet_async` arms. Phase 2
also ends green on `make verify`, which runs `check-generated`. Each phase goes through the
Actor/Critic loop (`agent-roles.md`); the Manager assigns agent numbers when a phase starts.
Phases 1, 3, 4, 5 and 6 are Mode A (`dotnet-actor` / `dotnet-critic`). Phase 2 is Rust tooling
(`actor-executor` / `kafka-critic`, reviewed against root `CLAUDE.md` and this plan), and its
generated `.cs` files are also reviewed by `dotnet-critic`.

### Phase 0 · Rule change (docs only, Manager plus owners)

- Apply D1's edits once the owners agree. Record every accepted decision of §3 in the rule text,
  so the Critics review against it; that includes the passages D1 lists for D6, D11, D14, D15,
  D19, D20 and D23.
- **Exit:** the owners approve the rule text. No code changes in this phase.

### Phase 1 · `KafkaException` base (C#)

- Add `internal virtual int FfiId` and make `Code` a non-virtual property returning it (D4, D14
  (i)). The public constructors give −1; the existing internal
  constructors keep their stored code until Phase 3. Update the `Code` doc.
- Add the core-path constructor `(CoreError, string?, Exception?)` and the `CoreError` marker
  (D3).
- Add the `JavaText` helpers (D21) and, with D22, the internal Java-message field. Switch
  `KafkaException(Exception cause)`, `WrapAtPoll` and the local-callback wrap from `CauseMessage`
  to `JavaText.ToString`, and delete `CauseMessage`.
- `IsRetriable` stays (D5 moves to Phase 3).
- **Tests:** update `KafkaExceptionCauseConstructorTests` and `KafkaExceptionTests`, which expect 0,
  and, with D22, `KafkaExceptionCauseConstructorTests.NullCause_MessageIsTheDefaultMessage`, which
  expects .NET's default text. Cover the core-path constructor and each `JavaText` rule (a set, a
  map, a double, `null`, a null element, a boolean, a `long` under a non-invariant culture, an
  exception with and without a message).
- **Exit:** builds on all three targets (`netstandard2.0`, `net8.0`, `net10.0`). No behaviour
  change other than D4 for the public constructors, D21's cause message and D22.

### Phase 2 · The generator and the generated classes (Rust xtask)

- **Renderer.** The C# renderer reuses `build_graph`, `Graph`, the `BRIDGE` and `JDK_BUILTINS`
  tables and the parsed Java classes (`java_parse::JavaClass`: constructors, fields, getters,
  singletons, factories). It does **not** reuse `Graph::eval` or `Effect`: those are Python source
  text, and an `Effect` flattens a whole constructor chain into one message, cause and field set,
  which C# cannot express (a subclass cannot assign its base's fields or set `Message` after the
  base constructor). Instead it translates each Java constructor into one C# constructor with the
  same `this`/`super` call and field assignments, through a C# expression translator, the
  counterpart of `Graph::translate`. The Python output stays byte-identical. Size the phase for a
  second translator, not a renderer over existing output.
- **Files.** For each class that is not in `CSHARP_HAND_WRITTEN`, and each JDK class D15 selects,
  it writes one `.cs` file at the folder and namespace `dotnet/CLAUDE.md` §2 gives its Java package
  (D19 for the JDK classes; for example `Common/Errors/TopicExistsException.cs`,
  `Consumer/CommitFailedException.cs`, `Common/Protocol/Types/SchemaException.cs`). Each file has:
  - the Apache header and a `GENERATED, DO NOT EDIT` marker;
  - XML docs naming the Java class; `GenerateDocumentationFile` is on and warnings are errors;
  - the Java parent as base class, written fully qualified (`global::`, D19), with `abstract` where
    Java's class is abstract;
  - the D3 constructors, the core-path constructor, the singletons and the getters (D3);
  - the `FfiId` override (D4);
  - messages built with `JavaText.ValueOf` for every operand of a Java `+` except a string literal
    and a `String`-typed conditional, as Python's `java_str` wraps them (a `String`-typed name
    included, so a null `groupId` prints `null`), and `JavaText.ToString(cause)` for a
    `(Throwable cause)` constructor (D21);
  - for `QuotaViolationException`, D16's message and Java-`toString()` override.

  The output is `dotnet format`-clean, so `dotnet format --verify-no-changes` and
  `check-generated` agree about the same files.
- **Registry.** It also writes `Internal/ErrorClasses.g.cs`: an internal id → factory table
  (D7; `KafkaException` and −27 have no entry, D18 and D8) and an internal type → id lookup for
  every class with a C id, including those that do not derive from `KafkaException` (the D15
  classes and `CorrelationIdMismatchException`, D6), used by the gRPC server, the soak client (D12)
  and `ErrorHierarchyTests`. A class with fields calls its fixed reader,
  `ErrorPayloadMarshal.Read<Class>`; this phase adds the hand-written `ErrorPayloadMarshal` with
  readers that build the class from the message only, so the registry compiles, and Phase 4 fills
  their bodies without a generator change.
- **Checks.** `check-generated` diffs the C# output. The Python-only parts of the generator
  (`generated_outputs`, `orphaned_outputs`, `unlisted_packages`) get C# counterparts: the orphan
  check walks `dotnet/src/Confluent.Kafka` and `dotnet/tests/Confluent.Kafka.UnitTests` (which holds
  the generated `ErrorClassRows.g.cs`) and flags a `.cs` there that carries the generated marker in
  its first 25 lines but that the Java sources no longer produce; `generate` deletes it.
- **Generation errors.** Generation stops on a constructor signature clash (D3, the core-path
  constructor included), a Java type with no C# mapping, or a class whose package has no folder
  rule.
- **`PublicNamespaceLayoutTests`.** It fails on the generated classes as it stands, so this phase
  updates it:
  - it keys exported types by namespace-qualified name, so the two `OffsetOutOfRangeException`s
    and the two `InvalidOffsetException`s that §2 prescribes can both appear (`Exported` uses
    `SingleOrDefault` today), and drops the "names below two namespaces" check, which §2's
    same-name rule contradicts;
  - it takes the error rows from the generator (an emitted row list, or the registry), instead of
    about 170 hand-added rows;
  - it gives the JDK classes their own namespace rule and source check (they are not under
    `kafka/`): a `Java.*` rule (D19).
- **Tests:** unit tests in xtask for the renderer, covering a plain class, an abstract base, a
  payload class, a static factory, a singleton, an abstract getter and its override, a signature
  clash, a message parameter Java names `s` or `msg`, and a set, a map and a double in a message.
  Class tests assert Java's text for a null `groupId` (`GroupAuthorizationException.ForGroupId`),
  a null configuration name (`ConfigException(name, value, message)`) and a set with a null
  element. Run `cargo xtask check-generated`.
- **Exit:** the generated classes compile in `dotnet build`; the existing .NET tests pass, with
  `PublicNamespaceLayoutTests` updated as above. Nothing constructs the new classes yet, apart
  from `SerializationException`, whose `Code` becomes −15, and the hand-written
  `RecordDeserializationException`, whose `FfiId` override (−27) lands in this phase.

### Phase 3 · Building the right class (C#)

- **`FromFfiError`.** Its last arm looks the id up in the registry. For −1 it applies D18. An
  unknown id falls back to a bare `KafkaException`, which reports −1 (D4). The carried-exception
  check, the `LocalCallbackError` unwrap and the cause recursion stay as they are. The wrap of a
  callback's exception, an earlier branch, resolves the core's id the way the last arm does, D18
  included: the core builds that wrap as a bare `KafkaError` (`Error::kafka_source`, −1 with
  `is_api_error` false), Java's `new KafkaException(t)`, so it gives a bare `KafkaException` with
  `JavaText.ToString(inner)` as its message, never `UnknownServerException`. The built-in arms
  build the D15 classes (−2, −3, −4, −5, −31) instead of the plain .NET built-ins, and −24 builds
  `CorrelationIdMismatchException`.
- **One reader.** Route `FromHandle` and `FromBorrowedHandle` through `FromFfiError`:
  - `NativeMethods.Code`/`Message` use `ThrowIfFfiFailed`;
  - `GroupDescriptionMarshal.CopyOutErrors` uses the borrowed path (`owned: false`), D18 included.

  Then delete the old reader.
- **D9, D10, D11, D20.** Apply them, including the gRPC server's two `Errors()` sites (D9).
  D11 moves the binding's last coded `KafkaException`s (7, 24) to their classes, so the stored
  code and the coded internal constructors are deleted: a bare `KafkaException` is always −1.
- **D5.** Remove `IsRetriable` and every use of it, including `NativeConsumer.cs`, the soak
  client's `ErrorIsRetriable` (→ `ex is RetriableException`) and the tests.
- **gRPC server.** `Translate.ToProto` sends the id of each D15 class (−2, −3, −4, −5, −31) and
  −24 for `CorrelationIdMismatchException`, by type, before any other arm, as Python's server sends
  `_ffi_id`. Without this, −24 (no longer a `KafkaException`) would go out as −4 between Phase 3
  and Phase 5. After D15 and D20 the arms are:
  - a D15 class or `CorrelationIdMismatchException`: its id, with its message;
  - a `KafkaException`: its `Code`, with its message;
  - an `ArgumentException` that is not a D15 class (a .NET-only precondition D20 keeps): −3, coded,
    as today;
  - everything else, a plain `InvalidOperationException` (the D11 invariant errors, Python's
    `RuntimeError`), `NotSupportedException` and `System.TimeoutException` included: the server's
    fallback, −4 with "dotnet server: <Type>: <message>", as Python's server sends an exception
    without an `_ffi_id`.

  Update `dotnet/CLAUDE.md` §7.6 to match.
- **New `ErrorHierarchyTests`:**
  - each class's base type matches its Java parent, read from `kafka/` as
    `PublicNamespaceLayoutTests` already does, checked in both directions;
  - abstractness matches Java;
  - each class's id, read through the generated type → id lookup (which covers the classes that
    have no `Code`), matches the C enum, read from
    `rust/target/include/confluent_kafka_bindings.h` (as `NativeAbiConformanceTests` reads it);
  - every C id except `NONE` and `LOCAL_CALLBACK` has a class, and every id but −1 and −27 has a
    registry entry.
- **New `FromFfiErrorClassTests`:**
  - real core errors built through the C factories give the right class:
    `kafka_common_Error_kafka_err` gives exactly `KafkaException` (D18's bare branch), and the
    `local_*` factories give the D15 classes (declare
    `kafka_common_Error_local_unsupported_operation_err` for −31);
  - D18's API branch has no C factory (none builds −1 with `is_api_error` true, and none takes a
    code), so it is tested through an internal seam that gives `FromFfiError`'s classification an
    id and an `is_api_error` value: −1 with `true` gives `UnknownServerException`, as Python tests
    it with a fake library;
  - the wrap of a carried non-Kafka exception (a listener or a serializer that throws one) is
    exactly `KafkaException`, not `UnknownServerException`;
  - every registry entry constructs its class;
  - an unknown id falls back to a `KafkaException` with `Code` −1;
  - an owner-check rejection and a core −2 have the same type (D20).
- **Updated tests.** These stop compiling or fail, and move to the new types:
  - `KafkaExceptionFfiErrorTests`, and every test that asserts a plain `KafkaException` for a core
    id;
  - the callers of the deleted reader: `KafkaExceptionReadTests.cs`, `KafkaExceptionTests.cs`,
    `KafkaExceptionCauseTests.cs`, `Interop/ManagedExceptionCarrierTests.cs`;
  - the exact-type assertions (`Assert.Throws`, `ThrowsAsync`, `IsType`) on the built-ins D15 and
    D20 replace, across `tests/`, `soak/` and `grpc-server/`: `NotSupportedException` (185 uses in
    47 files), `InvalidOperationException` (97 in 38), `ArgumentException` (196 in 55),
    `TimeoutException` (25 in 11). Each moves to the D15 class where the error is a core error or
    a D20 site, and stays on the .NET type where it is a precondition D20 keeps;
  - the 42 test files and the soak tests that use `IsRetriable` (D5).
- **Wire codes.** On the cross-language harness, codes change for `SerializationException` (−15,
  from Phase 2), `RecordDeserializationException` (−27), the owner check (−2, D20), the D15
  classes (now their own ids), a broker's `UNKNOWN_SERVER_ERROR` (still −1, now
  `UnknownServerException`, D18) and the binding's invariant errors (D11). Everything else is
  unchanged.
- **Exit:** the §1 example works against a mock or test-built errors, for every class without
  fields.

### Phase 4 · Extra fields (C#)

- **`ErrorPayloadMarshal`.** The generated registry already calls one fixed reader per class with
  fields, `ErrorPayloadMarshal.Read<Class>(message, inner, error)`, whose Phase 2 body builds the
  class from the message only. This phase replaces those bodies and changes no generated file, so
  it stays Mode A. Declare the views (their `_err` twins: the plain names are not in BH) and
  accessors in `NativeMethods`, then fill the fields of these classes:

  | Class | Field(s) |
  |---|---|
  | `TopicAuthorizationException` | `UnauthorizedTopics` |
  | `GroupAuthorizationException` | `GroupId` |
  | `InvalidTopicException` | `InvalidTopics` |
  | `ResourceNotFoundException` | `Resource` |
  | `DuplicateResourceException` | `Resource` |
  | `RecordTooLargeException` | `RecordTooLargePartitions` |
  | `ThrottlingQuotaExceededException` | `ThrottleTimeMs` |
  | `QuotaViolationException` | `Metric`, `Value`, `Bound` (D7) |
  | consumer `OffsetOutOfRangeException` | `OffsetOutOfRangePartitions`, and `Partitions` |
  | `LogTruncationException` | `DivergentOffsets`, and the inherited partitions |
  | `NoOffsetForPartitionException` | `Partitions`, as a set; the core's message is kept (D7) |
  | `RecordDeserializationException` | none: not reachable from .NET, and no reader (D7, D8) |
  | `CorrelationIdMismatchException` | −1 (no C accessor) |

- **Ownership.** The view (`kafka_common_Error_<x>_err`) is a `const` pointer borrowed from the
  error, even though its type has a `_destroy`: never destroy it. Each list or map accessor returns
  an owned container: copy it out with `CopyOutAndDestroy` (ffi §A2/§B2, classify by the accessor).
  The reader runs inside `FromFfiError`'s `try`, before the owned error is destroyed.
- **Tests.** Each class's route:
  - `TopicAuthorization`, `InvalidTopic`, `GroupAuthorization`, `ThrottlingQuotaExceeded` and
    `CorrelationIdMismatch`: a real core error built through its C factory;
  - the others have no C factory that builds a core error with their fields
    (`kafka_common_Error_record_too_large_err` takes a message only; the `*Error_new_err`
    functions return the payload type, which nothing converts into a `kafka_common_Error_t`): the
    reader is tested against a fake view source behind an internal seam, as Python tests its
    payload readers, plus a mock or broker-backed integration test wherever the core raises the
    error (the consumer offset errors, the admin resource errors).

  Each test checks each field, that the error handle is freed exactly once and that no view is
  destroyed. Include a non-ASCII topic name.
- **Exit:** every field Python reads is readable in .NET, through a core error, the seam or an
  integration test.

### Phase 5 · Callers and docs (C#)

- **D14.** Make `Code` internal (one modifier, no generated file changes) and add
  the `InternalsVisibleTo` entries for the assemblies `Confluent.Kafka.GrpcServer`, `SoakClient`
  and `SoakClient.Tests`, each with the shared public key.
  The gRPC server reads `Code` for a `KafkaException` and the registry's type → id lookup for every
  other class with an id (the D15 classes and `CorrelationIdMismatchException`), replacing Phase
  3's type arms with one lookup arm; the remaining arms are Phase 3's.
- **Soak client:** keep its checks identical to Python's (D12): the code tables, the coordinator
  and disconnect sets included, read through the internal `Code` and the id lookup, so −24 is read
  like any other id; retriability through `RetriableException` (done in Phase 3).
- **D23.** Wrap only an `InvalidCastException` thrown inside the serializer, with D23's .NET
  message. For an `ApiException`, do what the core's `handle_api_error` does (a2): call the
  delivery callback, if any, with placeholder metadata (offset −1) and the exception, and return a
  failed future, on both producer flavours (on `IAsyncProducer` the first stage completes and the
  future fails). Rethrow every other exception unchanged.
  - **Tests:** a serializer's `ApiException` reaches the callback once and the future's `Get()`
    (wrapped by D13 after Phase 6); a serializer's `InvalidCastException` throws
    `SerializationException` with D23's message; any other exception is thrown from `Send` as the
    same instance.
- **Docs:** the .NET section of `ERROR-CATEGORIZATION.md` (§6, §7, §10) and `STATUS.md`.
- **Exit:** `make verify-dotnet` green, including the multilanguage harness for the .NET arms.

### Phase 6 · `ExecutionException` from futures (C#)

- Apply D13 in `KafkaFutureImpl`, which today rethrows from one place for the sync futures and
  faults one `Task` for the async ones. Only the public `Get` / `GetNow` forms wrap; internal
  waits keep the raw error. A timeout throws the D15 `TimeoutException` (`KafkaFutureImpl.cs:155`,
  `:307`, `:391`); update the future docs (`KafkaFuture.cs`, `AsyncKafkaFuture.cs`).
- Split `ToCompletionStage()` from `Get()` on `AsyncKafkaFuture<T>` and `AsyncKafkaFuture`.
- **Callers:**
  - the gRPC server's roughly 50 `Get()` calls unwrap before `Translate.ToProto`, as
    `python/grpc_server.py` does with `ExecutionError`;
  - the soak client's 9 calls;
  - the unit tests that assert the raw error after `Get()`: at least 46 files and about 216
    single-line assertions.
- **Docs:** remove decision 18's deviation from `dotnet/CLAUDE.md` §4 and update the futures text.
- **Tests:** translate Java's `KafkaFutureTest` cases on failure (`ExecutionException` with the
  exact cause class) and on timeout (the D15 `TimeoutException`), for the sync and the async
  futures, as Python does for both. The timeout tests assert both texts D13 gives: no message
  (with D22, `Message` is `""`) for an ordinary future, and "Timeout after waiting for <ms> ms."
  for a send's future.
- **Exit:** `make verify-dotnet` green, including the multilanguage harness.

## 6 · Risks

- **Silent shadowing.** A type in the root `Confluent.Kafka` replaces a `System` type of the same
  name in every `Confluent.Kafka.*` file, with no diagnostic (D19). D19 (a) avoids it, and the
  generator writes base types fully qualified as well.
- **Name clashes (CS0104).** `Common.Errors.TimeoutException` versus `System.TimeoutException`, and
  `Common.Errors.AuthenticationException` versus `System.Security.Authentication.AuthenticationException`,
  clash in any file that imports both namespaces. Fix with qualification or an alias, as §2
  already prescribes for `TimeoutException`. The two `OffsetOutOfRangeException` and
  `InvalidOffsetException` classes clash the same way, and so does the D15 `TimeoutException`.
- **Analyzers.** `TreatWarningsAsErrors` with `AnalysisLevel=latest` may flag generated code, for
  example a missing standard exception constructor (CA1032, not on today). CA1724 is off (§2).
  Phase 2 must build clean without suppressions where it can, and state any suppression it adds.
- **Formatting.** The generated files are inside the `dotnet format` check; the renderer must emit
  them format-clean (Phase 2).
- **Exact-type tests.** xUnit's `Assert.Throws<T>` checks the exact type, so about 500 assertions
  on .NET built-ins are reviewed in Phase 3, one by one.
- **`netstandard2.0`.** The generated code uses only C# features available on all three targets
  (ffi §0.1). There are no `[Serializable]` constructors, as today.
- **Behaviour changes users can see.** All are listed as ⚠ in §3 (D4, D5, D6, D9, D11, D13, D14,
  D15, D18, D20, D21, D22, D23). Adding the classes is otherwise non-breaking: `catch
  (KafkaException)` still catches every Kafka class except `CorrelationIdMismatchException` (D6),
  and no longer catches the binding's invariant errors (D11); each D15 class is still its .NET
  built-in.
- **Drift.** A Java upgrade that adds an exception class fails `check-generated` until it is
  regenerated, which is the intended behaviour.

## 7 · Deviations from Java and Python that remain

With the decisions above, these remain inside the error hierarchy and the future failure path.
Each is forced by .NET or by the C ABI, or is kept from an earlier binding decision outside this
plan (the sync futures' missing `ToCompletionStage()`):

- `CorrelationIdMismatchException`'s ids read −1, because the C ABI does not expose them (D7).
  Python has the same deviation.
- **Bases.** The D15 classes and `KafkaException` have a .NET built-in parent in place of Java's
  `RuntimeException` or `Exception`, because .NET has neither `RuntimeException` nor multiple
  inheritance (D15). Python keeps the Java chain because its `RuntimeError` stands in for
  `RuntimeException` (`KafkaError(RuntimeError)`), and uses multiple inheritance only to add its own
  class too (`IllegalArgumentError(ValueError, RuntimeError)`, `IndexOutOfBoundsError`).
- **Placement.** The JDK classes live in `Confluent.Kafka.Java.*`, not at the root as in Python,
  because a root type silently shadows `System` types in every binding file (D19).
- `KafkaMetric` has no public constructor: Java's takes a `MetricValueProvider`, a `MetricConfig`
  and a `Time`, which .NET does not have (D7). Python has the constructor, typing those parameters
  as `object` and calling them by name (duck typing), which C# cannot do without the types. It
  matters only to code that builds a `QuotaViolationException` itself.
- `InterruptException`'s cause is an `Exception`, and it does not re-interrupt the thread (D24), as
  in Python.
- `QuotaViolationException.Message` is its `toString()` text after the class name, where Java's
  `getMessage()` is `null`, as in Python (D16).
- Java's `toString()` form names the .NET type, as Python's names its Python class (D21).
- A disposed `ConsumerHandle` throws `ObjectDisposedException`: the handle is a .NET object with
  no Java counterpart, and Python's is not public (D20). A closed client itself throws Java's
  `IllegalStateException`.
- The .NET-only preconditions, the sites where Java throws `NullPointerException` or nothing,
  throw `ArgumentNullException` (Python: `TypeError`) or `ArgumentOutOfRangeException`; every site
  where Java throws `IllegalArgumentException` uses the D15 class with Java's message (D20). The
  one exception is a negative timeout on a consumer lookup Java does not check (`Committed`,
  `PartitionsFor`, `ListTopics`, `OffsetsForTimes`, `BeginningOffsets`, `EndOffsets`): it throws
  the D15 `IllegalArgumentException` with the core's "Timeout must not be negative", the error the
  core's `_with_timeout` forms return (−3), as Python raises `IllegalArgumentError` there. Java's
  `calculateDeadlineMs` turns a negative timeout into no deadline instead.
- An RPC on a closed admin client throws `IllegalStateException` at the call, where Java's
  `KafkaAdminClient` returns the Result and fails its futures with it; the native handle is gone
  after `Close` (D20). Python has the same deviation.
- An error the client throws is never a singleton's `Instance`, where Java's internals raise the
  instance itself; compare by type (D3). Python's are not either.
- A serializer runs in the binding, before the core sees the record, so a serializer that throws
  one of Java's five fatal producer classes (`ProducerFencedException`, …) does not make the
  producer fatal, as Java's `maybeTransitionToErrorState` would. Python has the same gap. The
  transactional case belongs to the .NET transactions step (D23).
- A serializer's `ApiException` goes to the callback and a failed future, as Java and the Rust
  core do; Python throws it from `send()` (D23).
- No exception serialization, because .NET has made it obsolete (D17).
- The sync futures have no `ToCompletionStage()`, where Java's and Python's sync `KafkaFuture`
  have one. This is today's recorded deviation (`dotnet/CLAUDE.md` §4, Futures), unchanged by this
  plan.
- .NET naming forms only: `innerException` for Java's `cause`, `message` for Java's `s` / `msg`
  (as Python's Errors rule), properties for getters, `Instance` for `INSTANCE`, and
  `IReadOnlyCollection` / `IReadOnlyDictionary` for Java's collections.

Not deviations:
- the error classes Java does not mark `@InterfaceAudience.Public` are public, as Python's Errors
  rule makes every class the C enum names public (§2);
- `RecordDeserializationException` is hand-written where Python generates it (D8), but its
  constructor and getters are Java's non-deprecated constructor's, with `byte[]` for `ByteBuffer`
  as everywhere in the binding. It has no core-path constructor, so a core −27, which .NET never
  receives (D7), would fall back to a bare `KafkaException`.
