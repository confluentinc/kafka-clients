# Critic 116: review of `design/history/M22/P1-error-hierarchy-parity/PLAN.md` (.NET / C# lens) — resolved

Moved from `COMMENTS.116.md` after the plan revision; each finding ends with how and where it was resolved.

## 3. Medium · D13 vs D15 · the future timeout type contradicts the Java/Python alignment

**Problem.** D13 says "Unchanged, as in Java: a timeout still throws `System.TimeoutException`".
Java's `KafkaFuture.get(long, TimeUnit)` throws `java.util.concurrent.TimeoutException`, which D15
maps to the new root `TimeoutException`. Python's `KafkaFuture.get` raises the Java-named root
`confluent_kafka.TimeoutError`, not the plain built-in. So D13 keeps a deviation that D15 exists to
remove.

**Evidence.**
- `python/confluent_kafka/common/kafka_future.py:365-367`:
  `from confluent_kafka.timeout_error import TimeoutError as JavaTimeoutError … return JavaTimeoutError()`.
- `KafkaFutureImpl.cs:155`, `:307` and `:391` throw today's type.

**Fix.** In D13, write: "a timeout throws the D15 `TimeoutException` (Java's
`java.util.concurrent.TimeoutException`), which is still a `System.TimeoutException`". Update the
future docs (`KafkaFuture.cs:89`, `:283`; `AsyncKafkaFuture.cs:98`, `:275`) in Phase 6.

Resolved: D13 now says a timeout throws the D15 `java.util.concurrent` `TimeoutException`, with no message, a `System.TimeoutException`; Phase 6 changes `KafkaFutureImpl.cs:155,307,391` and the future docs and tests it.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 5. Medium · Phase 1 ordering · `IsRetriable` is removed before any class can replace it

**Problem.** Phase 1 removes `IsRetriable` "including the soak client". D5 says the soak's check
becomes `ex is RetriableException`, and Phase 1 also moves the soak checks to classes (D14). But
`RetriableException` and the other classes appear only in Phase 2, and core errors are built as
those classes only in Phase 3. Phase 1 therefore either does not compile, or leaves every error
non-retriable through Phases 1–2, which changes the soak's poll-failure limit and its send retries.
`KafkaExceptionFfiErrorTests` asserts the retriable flag of core errors, and those assertions have
no replacement until Phase 3.

**Evidence.** `soak/SoakClient/SoakClient.cs:310`, `:315`, `:650` and `:825` use
`ErrorIsRetriable`. `KafkaExceptionFfiErrorTests.cs` has 7 uses.

**Fix.** Move the removal of `IsRetriable`, the soak changes and the D14 soak class checks to Phase 3,
after `FromFfiError` builds the classes. Alternatively, keep `IsRetriable` in Phase 1 and drop it in
Phase 3. Then Phase 1's exit ("no behaviour change other than D4, D5 and D14") becomes true.

Resolved: D5's removal, the soak `ErrorIsRetriable` change and the tests moved to Phase 3, after `FromFfiError` builds the classes (D5 "Implementation (revised)", Phase 1 keeps `IsRetriable`); D14 moved to Phase 5.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 6. Medium · D12 vs D14 · the soak plan contradicts "identical to Python" and misses two code tables

**Problem.**
- D14 says the soak's code checks "become class checks", and names three of them. The soak also
  classifies errors with `s_coordinatorErrorCodes` and `s_disconnectErrorCodes`.
- Python's soak does not use class checks. It reads the private id (`_ffi_id`) of any error class
  and keeps its code tables. So D12's "keep .NET's check identical to Python's" means keeping the
  code tables and reading the internal `Code`, not class checks.
- D12's fallback, a change to the Python client, conflicts with §4's "no change to … the Python
  binding".
- Making `Code` internal also needs friend access that does not exist today: the library grants
  internals only to the unit-test assemblies.
- `dotnet/CLAUDE.md` §7.6 says the server "calls only the public API".

**Evidence.**
- `SoakClient.cs:58-75` (the coordinator and disconnect tables), used at `:1091-1104`;
  `SoakClient.cs:306` (`ErrorCode`).
- `python/soak/soakclient.py:365-379`: `error_code` reads `type(ex)._ffi_id`.
- `Confluent.Kafka.csproj:94-95`: `InternalsVisibleTo` only for `Confluent.Kafka.UnitTests` and
  `Confluent.Kafka.GrpcServer.UnitTests`.
- `soak/SoakClient.Tests/SoakTestSupport.cs:71` and `TopicSetupTests.cs:175`, `:318` assert `.Code`.

**Fix.** Make D14 grant `InternalsVisibleTo` to `Confluent.Kafka.GrpcServer`, `Confluent.Kafka.Soak`
and the soak tests. Every project is signed with the same key (`Directory.Build.props`), so this
works. Keep all four soak code tables reading the internal `Code`, as Python reads `_ffi_id`. Delete
"become class checks" and D12's Python-change alternative. Add the §7.6 "only the public API"
sentence to the docs D14 changes.

Resolved: D12 rewritten (Python reads `_ffi_id` and keeps its tables, now listed: auth, topic-exists, coordinator, disconnect); the Python-change alternative is gone; D14 grants `InternalsVisibleTo` to the gRPC server, soak and the soak tests and amends §7.6's "only the public API"; D1 lists that §7.6 sentence.

Verified by Critic 116 (round 2): the revised plan fixes this. The `InternalsVisibleTo` names in D14 are wrong, but that is a new error, not this one. It was carried over from my own suggestion and is now finding 20.

## 7. Medium · D4 / Phase 1 · `Code` cannot reach a subclass of a concrete generated class

**Problem.** The plan adds only `private protected KafkaException(int code, …)`, and says "each
generated class passes its own id". That works only for direct subclasses of `KafkaException`. In
Java, many concrete classes have concrete subclasses:
- `ApiException` (−6), `AuthenticationException` (−7), `AuthorizationException` (−9),
  `SerializationException` (−15), the consumer `OffsetOutOfRangeException` (−22) and
  `InvalidConfigurationException`.

A subclass's C# constructor chains to its parent's public constructor, which sets the parent's id.
The hand-written `RecordDeserializationException` shows the problem: it chains to
`SerializationException(message, cause)`, so once `SerializationException` is generated it reports
−15 instead of −27. The Phase 3 hierarchy test ("the hierarchy test still checks their parents and
ids", D8) would then fail.

**Evidence.**
- `Common/Errors/RecordDeserializationException.cs:43`: `: base(message, cause)`.
- BH:359: `SERIALIZATION = -15`; BH:371: `RECORD_DESERIALIZATION = -27`; BH:351-353 for −7 and −9.

**Fix.** Say that the generator gives every generated non-sealed class a
`private protected X(int code, …)` constructor that forwards the id, for each of its constructor
shapes, and that `RecordDeserializationException` calls it with −27. Alternatively, compute `Code`
from the runtime type through the generated registry, as Python's class-level `_ffi_id` does. Then
no constructor carries the id.

Resolved with the per-class override: D4 "Implementation (revised)" makes `Code` a `virtual` member each generated class overrides and `RecordDeserializationException` overrides with −27; no constructor carries the id (Phase 1, Phase 2).

Round 2: the overridden member is now the internal `FfiId`, with `Code` a non-virtual property returning it (D4, D14 (i), 117#14); under D14 (ii) it stays `Code`, its accessibility fixed in Phase 1.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 8. Medium · D3 / D7 · the "internal core constructor" collides with Java's `(String, Throwable)` constructor

**Problem.** D3 adds "one `internal` constructor that the core path uses", and D7's registry calls
`(message, inner) => new X(…)`. Most classes already get a public `(string? message,
Exception? innerException)` from Java's `(String, Throwable)` constructor. An internal constructor
with the same parameter types is CS0111, because accessibility does not distinguish overloads.
Payload classes also need message, cause and payload together, and Java has no such constructor
(for example, `TopicAuthorizationException` has no `Throwable` constructor at all).

**Evidence.** Java `(String, Throwable)` constructors: `InvalidTopicException.java:43`,
`ResourceNotFoundException.java:46`, `RecordTooLargeException.java:38` and
`SerializationException` (4 constructors). `TopicAuthorizationException.java:28-37` has none.

**Fix.** Give the internal constructor's signature in D3. For example, a leading internal marker
parameter (`internal X(CoreError _, string? message, Exception? inner, …payload)`) that no Java
constructor can produce. Have the registry call only that constructor.

Resolved: D3 "Implementation (revised)" gives the core-path constructor's signature, `internal X(CoreError _, string?, Exception?, …fields)` (callable by the registry and by subclasses in the assembly); the clash check covers it; D7's registry calls it.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 9. Medium · D3 type table · incomplete, so generation stops on its own rule

**Problem.** D3's table maps four Java types, and Phase 2 says generation stops on "a Java type with
no C# mapping". The payload classes' public constructors and getters use types the table does not
list. Two members also need logic the plan does not mention:
- `InvalidOffsetException` declares `abstract Set<TopicPartition> partitions()`, which the
  generator must emit as an abstract member.
- `OffsetOutOfRangeException.partitions()` returns `offsetOutOfRangePartitions.keySet()`, a
  computed getter, not a field.

**Evidence.**
- `int`: `ThrottlingQuotaExceededException(int, String)` (`:33`) and `CorrelationIdMismatchException`.
- `Object`: `ConfigException(String, Object)` and `(String, Object, String)` (`:35`, `:39`).
- `TopicPartition` and `Collection<TopicPartition>`: `NoOffsetForPartitionException.java:43` and `:53`.
- `Map<TopicPartition, OffsetAndMetadata>`: `LogTruncationException.java:45` and `:57`.
- `KafkaMetric` and `double`: `QuotaViolationException.java:34`.
- `Set<TopicPartition>` returns: `InvalidOffsetException.java:49` and
  `OffsetOutOfRangeException.java:65`.

**Fix.** Complete the D3 table with these types:
- `int` → `int`, `double` → `double`, `Object` → `object?`;
- `TopicPartition` and `Collection<TopicPartition>` → `IReadOnlyCollection<TopicPartition>`;
- `Map<TopicPartition, OffsetAndMetadata>` → `IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>`;
- `KafkaMetric` → see finding 10.

Also say that the renderer translates abstract getters and Java getter bodies, as the Python
renderer does.

Resolved: D3's table now has `Object`, `int`/`long`/`double`, `TopicPartition`, `Collection`/`Set<TopicPartition>`, `Map<TopicPartition, OffsetAndMetadata>`, and points `KafkaMetric` to D7 and `InterruptedException` to D24. `TopicPartition` maps to `TopicPartition`, not a collection. Abstract getters, overrides and `keySet()` views are in D3 "Implementation (revised)".

Verified by Critic 116 (round 2): the revised plan fixes this.

## 10. Medium · D7 · `QuotaViolationException.Metric` as `IMetric` is an unrecorded deviation, and the C claim is inexact

**Problem.**
- Java's `metric()` returns a `KafkaMetric`, a class in `common.metrics`, and Java's only constructor
  takes one. Python has a `KafkaMetric` class and uses it for both.
- .NET has no `KafkaMetric`, only the interface `IMetric` (Java's `Metric`). With D7, `Metric` has
  the type `IMetric`, and the public constructor has no type to take. That is a deviation, and §7
  does not list it.
- D7 also says C exposes "only the metric's name and group". It actually exposes a full borrowed
  `MetricName_t` (name, group, description, tags). It is Python that reads only the name and group.

**Evidence.**
- `QuotaViolationException.java:34`, `:40`.
- `python/confluent_kafka/common/metrics/quota_violation_error.py:31`, `:50`, `:60`.
- `dotnet/src/Confluent.Kafka/Common/IMetric.cs:24`; no `class KafkaMetric` under `dotnet/src`.
- BH:26172: `kafka_common_QuotaViolationError_metric_name_err` returns `const kafka_common_MetricName_t *`.
- `python/_confluentkafka.c:3892-3906`.

**Fix.** Choose one in D7:
- (a) Add `Confluent.Kafka.Common.Metrics.KafkaMetric`, as Python does, and use it for the property
  and the constructor.
- (b) Keep `IMetric`, and list it in §7 as a deviation.

Correct the C sentence, and state that the reader copies the whole `MetricName` through the
existing metric marshalling, or only the name and group as Python does.

Resolved in D7: the C sentence is corrected (full `MetricName` exposed; Python reads name and group), .NET reads all four fields, and the options (a) a `KafkaMetric` class without a public constructor (recommended) and (b) `IMetric` are given, each with its §7 entry.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 11. Medium · D16 · a `ToString()` override is the wrong .NET form of Java's `toString()` override

**Problem.** In .NET, `Exception.ToString()` produces the type, the message, the inner exceptions and
the stack trace that loggers and the unhandled-exception handler print. Replacing it with Java's
one-line text removes the stack trace and the cause chain from every log of
`QuotaViolationException`. Python translates the override into `__str__`, which is its form of
the message: "the override's text after the class name". Java's `getMessage()` is `null` for this
class. Without a message argument, .NET's `Message` would read "Exception of type '…' was thrown.".
The binding's `toString()` form (`CauseMessage`, used for D13's `ExecutionException` message and for
the `(Exception cause)` constructors) is built from `Message`, so it would not pick up an override
of `ToString()` either.

**Evidence.**
- `QuotaViolationException.java:53-61` (the `toString()` override) and `:34-38` (no `super(message)`).
- `python/confluent_kafka/common/metrics/quota_violation_error.py:73-80`.
- `Common/KafkaException.cs:95-96`: `CauseMessage` is `GetType().FullName + ": " + cause.Message`.

**Fix.** Rewrite D16: the renderer passes the override's text after the class name as the base
`message`, and does not override `ToString()`. Then D13's `ExecutionException` message and every
`CauseMessage` match Java's `toString()`.

Resolved: D16 rewritten: no `ToString()` override; `Message` is the override's text after the class name (Python's choice, recorded in §7), and the class overrides D21's internal Java-`toString()` so wrapping messages carry Java's text.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 12. Medium · Phase 2 exit · "the existing .NET tests still pass" is false as written

**Problem.** `PublicNamespaceLayoutTests` fails as soon as the generated classes are exported:
- it needs a hand-written row for every exported type, so about 165 rows are missing;
- it rejects two exported types with the same name below their namespaces. Java has
  `OffsetOutOfRangeException` and `InvalidOffsetException` in both `common.errors` and
  `clients.consumer`, and D15 adds a second `TimeoutException`;
- it asserts that no type is in the root namespace;
- it looks up each row's Java class under `kafka/`, where the JDK classes are not.

Phase 2 does not plan any change to this test. D15 mentions only the root-namespace assertion.

**Evidence.** `tests/Confluent.Kafka.UnitTests/PublicNamespaceLayoutTests.cs`:
- `:341-353`: "Exported types without a row", and "Exported names below two namespaces" through
  `Duplicates`;
- `:355-357`: `NoPublicType_IsInTheRootNamespace`;
- `:423-447`: `TheJavaClass_ExistsInTheKafkaSources`;
- `:474`: `NameBelowNamespace` strips the namespace.

**Fix.** Make updating this test a Phase 2 deliverable:
- have the generator emit the rows, or have the test read the generated registry;
- key rows on the full name, or allow the two documented same-name pairs;
- add D15's root types to `JavaCounterparts` as JDK rows, which the test skips when it searches
  `kafka/`.

Resolved: Phase 2 has a `PublicNamespaceLayoutTests` step (full-name keys, generator-fed rows, a JDK namespace rule and source check), and its Exit says the tests pass with that update.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 13. Medium · Phase 3 tests · exact-type assertions on .NET built-ins break under D15, and the plan does not list them

**Problem.** xUnit's `Assert.Throws<T>` and `ThrowsAsync<T>` check the exact type. Once −2, −3, −4,
−5 and −31 are built as D15 subclasses, every exact assertion on a core error fails. Phase 3 lists
only `KafkaExceptionFfiErrorTests` and the tests that assert a plain `KafkaException`.

**Evidence.** Across `tests/`, `soak/` and `grpc-server/`:
- `Throws*<NotSupportedException>` or `IsType`: 185 uses in 47 files. Examples:
  `PublicAdminListConsumerGroupOffsetsTests.cs:85` and `:109`, the core −31 "Not implemented yet".
- `InvalidOperationException`: 97 uses in 38 files. Example: `ConsumerCommitCallbackBridgeTests.cs:403`,
  the core −2.
- `ArgumentException`: 195 uses in 55 files.
- `TimeoutException`: 25 uses in 11 files.

Many of these are binding preconditions, which finding 4 affects too.

**Fix.** Phase 3 should list these suites, with counts, as Phase 6 does for the futures. It should say
which assertions move to the D15 type and which stay on the precondition type. Then Phase 3's exit,
"make verify green", can be met.

Resolved: Phase 3 "Updated tests" lists the exact-type assertion suites with counts (185/47, 97/38, 196/55, 25/11, rechecked) and the rule for which move to the D15 class and which stay; §6 adds an exact-type risk.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 14. Medium · Phase 4 tests · "built from a core error through that class's C factory" is impossible for seven classes

**Problem.** The C ABI has no factory that returns a `kafka_common_Error_t` for these classes:
- `DuplicateResourceException`
- `ResourceNotFoundException`
- `QuotaViolationException`
- the consumer `LogTruncationException`
- `NoOffsetForPartitionException`
- the consumer `OffsetOutOfRangeException`
- `RecordDeserializationException`

Each has only a `kafka_common_<X>Error_new_err`, which returns the payload type. That type cannot be
handed to `FromFfiError`. Also, because .NET passes `NULL` deserializers (ffi §B4), the core never
produces a `RecordDeserializationError` for .NET. That reader is reachable only in theory.

**Evidence.**
- Error factories exist only for `topic_authorization`, `invalid_topics`, `group_authorization`,
  `throttling_quota_exceeded`, `record_too_large` and `correlation_id_mismatch` (BH:21796-22219).
- Payload-type constructors: BH:25608 (DuplicateResource), BH:25021 (LogTruncation),
  BH:25194 (NoOffsetForPartition), BH:25331 (OffsetOutOfRange), BH:26156 (QuotaViolation),
  BH:26348 (RecordDeserialization).
- No function converts a payload type into a `kafka_common_Error_t`: the only
  `kafka_common_Error_t`-returning functions named after these types are their borrowed
  `_source_err` accessors.

**Fix.** Give each class its test route:
- the six with an Error factory: as written;
- `NoOffsetForPartitionException` and the offset errors: a mock consumer run that makes the core
  report the error, if one exists, otherwise a broker-backed integration test;
- the admin resource errors: a `MockAdminClient` call;
- `RecordDeserializationException`: say the core path is not reachable from .NET and test only the
  binding-built form.

Phase 4's exit then needs "or an integration test" for the classes the mocks cannot produce.

Resolved: Phase 4 Tests gives each class its route: the five with a C factory as before, the rest through a fake view source behind a seam (as Python tests its readers) plus mock or integration tests; `RecordDeserializationException` is marked unreachable from .NET (D7). The Exit allows the seam or an integration test.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 15. Medium · Phase 4 ownership · "freed by its owner" does not say who the owner is

**Problem.** These views mix both ownership categories, and ffi §A2/§B2 say to classify by the
accessor:
- The view (`kafka_common_Error_<x>_err`) is a `const` pointer borrowed from the error, even though
  its type has a `_destroy`. Freeing it is a double free.
- The accessors for lists and maps are non-`const`, and the reader owns what they return. It must
  free it with `kafka_List_destroy_err` or `kafka_Map_destroy_err`.
- For an owned error, all of this must happen before `FromFfiError`'s `finally` destroys the error.

"Following the existing marshal patterns" does not settle this: `CopyOutAndDestroy` is for owned
roots, and `CopyOut` is for borrowed ones.

**Evidence.**
- BH:27031 and BH:25759: the views return `const kafka_common_TopicAuthorizationError_t *` and the
  equivalent.
- BH:25061-25063: "The returned list is owned, freed with `kafka_List_destroy`"
  (LogTruncation `partitions`); likewise `offset_out_of_range_partitions` and `divergent_offsets`
  (BH:25083-25099).
- `KafkaException.cs:190-196`: the owned error is destroyed in the `finally`.

**Fix.** State the rule in Phase 4:
- the view is borrowed: never destroy it;
- each list or map accessor returns an owned container: copy it out with `CopyOutAndDestroy`;
- the reader runs inside `FromFfiError`'s `try`, before the destroy.

Add a test that asserts no `_destroy` is ever called on a view.

Resolved: Phase 4 has an "Ownership" bullet (borrowed `const` view never destroyed, owned list/map accessors through `CopyOutAndDestroy`, reader inside `FromFfiError`'s `try`) and a test that no view is destroyed.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 16. Medium · D9 · incomplete: other admin surfaces are also narrowed to `KafkaException`, and the gRPC server breaks in Phase 3

**Problem.** D9 fixes only `ListGroupsResult.Errors()`. Other surfaces have the same narrowing:
- `ElectLeadersResult.Partitions()` and its async peer are typed `KafkaException?`. Java's is
  `Optional<Throwable>`, and the marshal fails the whole future when an error is not a
  `KafkaException`.
- Three surfaces are typed `KafkaException` where Java's type is `ApiException`, which now exists:
  `DeleteAclsResult.FilterResult.Exception`, `LogDirDescription.Error`, and the public
  `CreateTopicsResult.TopicMetadataAndConfig(KafkaException)` constructor.
- The gRPC server declares `IReadOnlyCollection<KafkaException> errors = result.Errors()…`, so D9
  breaks its build in Phase 3, although the gRPC work is planned for Phase 5.

**Evidence.**
- `Admin/ElectLeadersResult.cs:61`, `Admin/AsyncElectLeadersResult.cs:61`;
  `Internal/Interop/ElectLeadersMarshal.cs:20-27`; Java `ElectLeadersResult.java:48`.
- `Admin/DeleteAclsResult.cs:134` (Java `DeleteAclsResult.java:60`, `ApiException`).
- `Admin/LogDirDescription.cs:138` (Java `LogDirDescription.java:35-44`).
- `Admin/CreateTopicsResult.cs:218` (Java's constructor takes `ApiException`).
- `grpc-server/AdminServiceImpl.cs:1153`, `grpc-server/AsyncAdminServiceImpl.cs:1156`.

**Fix.** Extend D9:
- `ElectLeadersResult.Partitions()` becomes `IReadOnlyDictionary<TopicPartition, Exception?>`, and
  the marshal no longer fails the future;
- the three `ApiException` surfaces are retyped to `ApiException`, or the plan records why not.

Mark both as breaking changes (⚠), and move the gRPC server's two `Errors()` sites into Phase 3.

Resolved: D9 now covers `ElectLeadersResult.Partitions()` (`Exception?`, no whole-future failure) and retypes `DeleteAclsResult.FilterResult.Exception`, `LogDirDescription.Error` and `TopicMetadataAndConfig` to `ApiException` (⚠); the gRPC `Errors()` sites move to Phase 3.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 17. Low · D1 / Phase 0 · the list of rule-text edits leaves out sentences D6, D11 and D15 contradict

**Problem.** D1's list misses rule text that the other decisions contradict:
- in ffi §B5, the wakeup bullet ("**Not** a subclass (decision 14)") and the anti-pattern "Making
  `wakeup()` a distinct exception subclass now";
- the §B5 `GroupMetadata()` bullet, which throws "the INVALID_GROUP_ID (24) `KafkaException`" (D11);
- the §B5 rule that maps "-2 and -4 → `InvalidOperationException` … -31 → `NotSupportedException`"
  (D15), and §B1's "−31 … a `NotSupportedException`";
- ffi §A7: "a retriable `KafkaException` with code 7 … not `System.TimeoutException`, which
  decision 14 keeps for -5" (D11, D15);
- §B5's tests that assert `IsRetriable` (D5);
- `dotnet/CLAUDE.md` §4's `GroupMetadata()` text and §2's "there is no root exception type" (D15
  mentions only the latter).

Phase 0 asks the Critics to review against the rule text, so any sentence left unedited keeps
grading the old model.

**Fix.** Add these sentences to D1's list, or to Phase 0's "record every accepted decision" bullet.

Resolved: D1's passage list now includes the ffi §B5 wakeup bullet and anti-pattern, the `GroupMetadata()` bullet, the local-codes bullet, §B1's −31 sentence, ffi §A7's code-7 text, §B5's `IsRetriable` tests, and `dotnet/CLAUDE.md` §2/§3/§4 passages; Phase 0 records them.

Verified by Critic 116 (round 2): the revised plan fixes this.

## 18. Low · factual counts and sources

- D5 says "about 25 unit-test files". The count is 39 under `tests/Confluent.Kafka.UnitTests`, plus
  3 under `tests/Confluent.Kafka.GrpcServer.UnitTests` and `soak/SoakClient.Tests`
  (`grep -l 'IsRetriable\|isRetriable'`).
- D15 renders "from `JDK_BUILTINS`". That table has eight entries, including
  `java.util.NoSuchElementException` and `java.lang.IndexOutOfBoundsException`
  (`rust/xtask/src/error_hierarchy.rs:637-701`), and D15's table lists six. Say whether the C#
  renderer skips those two.
- Phase 3's hierarchy test reads the enum from `rust/target/include/confluent_kafka.h`. The binding's
  header is `confluent_kafka_bindings.h` (ffi §0.1), which is what `NativeAbiConformanceTests.cs:37`
  reads. Use that one.

Resolved: D5 now says 39 unit-test files, 3 GrpcServer test files and the soak tests (rechecked); D15 explains the two `JDK_BUILTINS` entries it skips and why; Phase 3 reads the enum from `confluent_kafka_bindings.h`.

Verified by Critic 116 (round 2): the revised plan fixes this.

# Round 2 (verification of the revised plan)

## 19. Low · D16 / D22 · D22 contradicts D16 for `QuotaViolationException`, and changes today's public `Message` without a ⚠

**Problem.**
- D22 lists `QuotaViolationException` as a class whose Java `getMessage()` is `null`. Under its
  option (a), `Message` is then `""`. D16 instead makes `QuotaViolationException.Message` the
  `toString()` override's text after the class name, and §7 records that. Both decisions are
  recommended, so for this class the plan prescribes two different values of `Message`.
- D22 (a) also changes public behaviour that ships today. `new KafkaException()` and
  `new KafkaException((Exception?)null)` return .NET's "Exception of type
  'Confluent.Kafka.Common.KafkaException' was thrown." today. After D22 they return `""`. D22 has
  no ⚠ and is not in §6's list of visible changes (D4, D5, D6, D9, D13, D14, D15, D18, D20, D23).
  §3 says every ⚠ item goes in the release notes.

**Evidence.**
- Plan D22: "Java's `getMessage()` can be `null` (a no-argument constructor,
  `QuotaViolationException`, a timed-out `get`) … `Message` returns `""` for a Java `null`".
- Plan D16: "the class's `Message` is the override's text after the class name". §7: "`QuotaViolationException.Message`
  is its `toString()` text after the class name".
- `src/Confluent.Kafka/Common/KafkaException.cs:44` has the parameterless constructor, and `:71`
  has `KafkaException(Exception? cause)` with `CauseMessage(null)` → `null` → .NET's default text.
- `tests/Confluent.Kafka.UnitTests/KafkaExceptionCauseConstructorTests.cs:77-93`
  (`NullCause_MessageIsTheDefaultMessage`) asserts that default text for `KafkaException` and
  `SerializationException`.

**Fix.**
- Say in D22 that a class with a D16 override is excepted: its `Message` is D16's text. Or drop
  `QuotaViolationException` from D22's list.
- Mark D22 ⚠ and add it to §6's list.
- In Phase 1, name the test that changes (`NullCause_MessageIsTheDefaultMessage`).

Resolved (with 117#20 and 108#22): D22 now excepts the D16 class, so `QuotaViolationException.Message` stays D16's text, and D22 is marked ⚠ and listed in §6; Phase 1's Tests bullet names `KafkaExceptionCauseConstructorTests.NullCause_MessageIsTheDefaultMessage`, which changes.

## 20. Low · D14 · the soak friend-assembly names are wrong

**Problem.** D14 gives `InternalsVisibleTo` to "`Confluent.Kafka.GrpcServer`,
`Confluent.Kafka.Soak` and the soak tests". `Confluent.Kafka.Soak` is a namespace, not an
assembly. The soak assemblies are `SoakClient` and `SoakClient.Tests`. On a strong-named library,
`InternalsVisibleTo` must name the friend's real assembly name. Otherwise `Code` stays
inaccessible, and the soak client's code tables and the soak tests' `Code` assertions stop
compiling, which D14 says they keep doing. My round-1 finding 6 used the same wrong name, so this
corrects it.

**Evidence.**
- `soak/SoakClient/SoakClient.csproj:34`: `<AssemblyName>SoakClient</AssemblyName>`.
- `soak/SoakClient.Tests/SoakClient.Tests.csproj:24`: `<AssemblyName>SoakClient.Tests</AssemblyName>`.
- `soak/SoakClient/SoakClient.csproj:62` already uses the real name for its own friend:
  `InternalsVisibleTo Include="SoakClient.Tests"`.
- `grpc-server/Confluent.Kafka.GrpcServer.csproj`: `Confluent.Kafka.GrpcServer` is correct.

**Fix.** In D14 and Phase 5, name `Confluent.Kafka.GrpcServer`, `SoakClient` and `SoakClient.Tests`.

Resolved (with 108#12): D14 and Phase 5 name the assemblies `Confluent.Kafka.GrpcServer`, `SoakClient` and `SoakClient.Tests`, each with the shared public key, and D14 notes that `Confluent.Kafka.Soak` is only the soak client's root namespace.

## 21. Low · Phase 3 · the local-callback wrap must also go through D18

**Problem.**
- Phase 3 applies D18 in `FromFfiError`'s last arm only ("For −1 it applies D18").
- The wrap of a callback's exception is a separate, earlier branch. The plan says it "builds the
  class of the core's id", and that id is −1.
- The core builds the wrapper with `Error::kafka_source`, which is `Errors::UnknownServerError`
  (−1) on the bare `KafkaError` variant, not an API error.
- Looked up in the registry without D18's check, −1 is `UnknownServerException`, an
  `ApiException`. Java's `new KafkaException(t)` is a bare `KafkaException`, which is also what
  Python's `_build` gives for a non-API −1.
- Any `catch (ApiException)` would then catch a user callback's failure.

**Evidence.**
- `rust/src/common/error.rs:2842-2844`: `kafka_source` → `KafkaError::with_source(Errors::UnknownServerError, source)`.
- `src/Confluent.Kafka/Common/KafkaException.cs`, in `FromFfiError`: the `WrapsItsSource` branch
  returns before the `code switch`, so it does not reach the last arm.
- Plan Phase 3, the `FromFfiError` bullet.

**Fix.** In Phase 3, say that the callback-wrap branch resolves −1 through D18, as the last arm
does. Or say it always builds a bare `KafkaException`, as Java's `new KafkaException(t)` does.
Add a test: a listener or serializer that throws a non-Kafka exception gives exactly
`KafkaException`, not `UnknownServerException`.

Resolved (with 117#17): Phase 3's `FromFfiError` bullet says the callback-wrap branch resolves the core's id as the last arm does, D18 included, so the core's `Error::kafka_source` wrap (−1, `is_api_error` false) gives a bare `KafkaException`, never `UnknownServerException`; `FromFfiErrorClassTests` asserts that the wrap of a carried non-Kafka exception is exactly `KafkaException`.

## 22. Low · D1 / D20 · D1's passage list misses the rule text that D20 changes

**Problem.**
- D20 moves the negative-timeout check ("The timeout cannot be negative.") from
  `ArgumentOutOfRangeException` to the D15 `IllegalArgumentException`, because it translates a
  Java `IllegalArgumentException`. Java's `Timer` also throws `IllegalArgumentException` for the
  consumer's negative timeout.
- The rule text says `ArgumentOutOfRangeException` in several passages, and D1's list ("the
  places that state it") names none of them.
- So after Phase 0 the rulebook would still require the type that D20 removes.

**Evidence.**
- `dotnet/CLAUDE.md:408` and `:488` (the §3 interface sketches): "every TimeSpan: negative →
  ArgumentOutOfRange".
- `dotnet/CLAUDE.md:653`, the §4 Disposal row: "A negative timeout throws
  `ArgumentOutOfRangeException` with Java's message" (twice).
- `.claude/rules/ffi-marshalling.md:740` and `:776` (§A5 Precondition bullet and its test).
- `.claude/rules/ffi-marshalling.md:1874` (§B5 Precondition bullet).
- `.claude/rules/ffi-marshalling.md:2442` (§C1 Teardown).

**Fix.** Add these passages to D1's list, tagged D20, or move them to Phase 0 with the other
rule-text edits.

Resolved: D1's passage list now names, for D20, `dotnet/CLAUDE.md`'s §3 sketch comments, the §4 Disposal row (both sentences) and the §4 `Seek` guard, and `ffi-marshalling.md`'s §A5 Precondition bullet and its test, §B5 Precondition bullet and §C1 teardown text; Phase 0 applies them with the other rule-text edits.

## 23. Low · D11 (a) / Phase 3 · a plain `InvalidOperationException` is not sent the way Python sends its `RuntimeError`

**Problem.**
- D11 (a) throws "a plain `InvalidOperationException` (sent as −4) where Python raises
  `RuntimeError`". The code matches, but the message does not.
- Python's `RuntimeError` has no `_ffi_id`, so its server sends it through the fallback, as −4
  with "python server: RuntimeError: …".
- The .NET server keeps a plain `InvalidOperationException` arm, which sends −4 with the bare
  message. Phase 3 keeps "the plain .NET built-in arms" after the D15 arms and does not decide
  which of them still have a reason to exist.
- After D15 and D20, a binding-made Java built-in is a D15 class. So the plain arms only see .NET
  idioms (`ObjectDisposedException`, the kept `ArgumentOutOfRangeException`) and the D11
  invariant errors. Each of those needs its own choice: coded, or the "dotnet server:" fallback.

**Evidence.**
- `grpc-server/Translate.cs:116-125`: `InvalidOperationException => Coded(LocalIllegalStateCode, error)`.
  The `ServerMade` fallback at `:159-163` adds "dotnet server: {Type}: {message}".
- `python/grpc_translate.py:180-199`: `_ffi_id` classes are sent with their code and `str(err)`.
  Any other exception is sent as −4 with "python server: {type}: {err}".
- Plan Phase 3, the gRPC server bullet: "before the plain .NET built-in arms".

**Fix.** In Phase 3, list the plain arms that remain and what each stands for. Send D11's
invariant errors through `ServerMade`, as Python sends `RuntimeError`. One way is a type that the
coded arms do not match. Then fix D11's "(sent as −4)" to say which message is sent.

Resolved: Phase 3's gRPC bullet now lists every remaining arm: a D15 class or `CorrelationIdMismatchException` by its id; a `KafkaException` by `Code`; `ObjectDisposedException` −4 coded; a non-D15 `ArgumentException` −3 coded; everything else, the D11 invariant `InvalidOperationException`s included, through the `ServerMade` fallback (−4, "dotnet server: <Type>: <message>"), as Python's server sends its `RuntimeError`. D11 (a) now says which message is sent.

## 24. Low · D3 · the singleton `Instance` is documented "for comparing", but no error from the client is ever that instance

**Problem.** D3 emits `public static readonly X Instance` and documents it as being "for
comparing and catching". `FromFfiError` builds a new object for every core error, so
`ex == DisconnectException.Instance` (or `ReferenceEquals`) is always false for an error that
`Poll`, `Get()` or `Send` throws. Java's internals do raise the instance itself
(`NetworkClientDelegate`, `ConsumerNetworkClient`), so in Java such a comparison can hold. The
plan's doc would invite a check that can never succeed.

**Evidence.**
- Plan D3, "Singletons".
- `kafka/clients/.../consumer/internals/NetworkClientDelegate.java`:
  `completeExceptionally(DisconnectException.INSTANCE)`.
- Plan Phase 3: `FromFfiError` "builds the class of the core's id" for each error.

**Fix.** Make the doc say that an error the client throws is never this instance, so compare by
type (`ex is DisconnectException`). List that in §7 next to the `Instance` naming line.

Resolved: D3's Singletons bullet now says an error the client throws is never this instance (`FromFfiError` builds a new object each time), so a caller compares by type; §7 records it.

## 25. Medium · D20 · `ProducerRecord`'s Java `IllegalArgumentException` checks are filed as .NET-only preconditions

**Problem.**
- D20 sorts binding throws into two groups:
  - translated Java checks, which get the D15 class;
  - .NET idioms, which are kept: "`ArgumentNullException` where Java throws
    `NullPointerException`", and "an `ArgumentOutOfRangeException` Java does not throw".
- `ProducerRecord`'s constructor fits neither group. Java throws `IllegalArgumentException` for a
  null topic ("Topic cannot be null.") and for a negative partition ("Invalid partition: %d.
  Partition number should always be non-negative or null.").
- .NET throws `ArgumentNullException` and `ArgumentOutOfRangeException` ("Partition must not be
  negative."), which is neither Java's type nor Java's message.
- Python raises `IllegalArgumentError` with Java's text for both (`producer_record.py:170`,
  `:177`), as its "Implementation over the FFI" exception 4 requires.
- Under D20's recommendation, these sites would be kept as .NET idiom and recorded in §7 as if
  Java had no check.
- The Phase 3 audit covers "the `ArgumentException` throws", but the recommendation keeps
  `ArgumentNullException` and `ArgumentOutOfRangeException` as a group. Whether a site is
  translated or .NET-only is never checked against Java for these two types.

**Evidence.**
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerRecord.java:72-79`.
- `src/Confluent.Kafka/Producer/ProducerRecord.cs:66-74`.
- `python/confluent_kafka/producer/producer_record.py:170-178`.
- Plan D20, the sites list and the recommendation. Plan §7, the ".NET-only preconditions" line.

**Fix.**
- In D20, classify by the Java method, not by the .NET type. Every `ArgumentNullException` and
  `ArgumentOutOfRangeException` site goes through the same Phase 3 audit. Where Java throws
  `IllegalArgumentException`, the site uses the D15 `IllegalArgumentException` with Java's message
  (`ProducerRecord`'s topic and partition checks, and the negative timeouts of finding 22). Only
  the sites where Java throws `NullPointerException`, or nothing at all, stay as .NET idiom in §7.
- Add `ProducerRecord`'s two checks to D20's site list.

Resolved: D20 now classifies each site by the Java method, not the .NET type: `ProducerRecord`'s topic and partition checks and the negative timeouts (`Timer`'s "Invalid negative timeout …" included) use the D15 `IllegalArgumentException` with Java's message; the Phase 3 audit covers every `ArgumentException`, `ArgumentNullException` and `ArgumentOutOfRangeException` throw; §7 keeps as .NET-only only the sites where Java throws `NullPointerException` or nothing.
