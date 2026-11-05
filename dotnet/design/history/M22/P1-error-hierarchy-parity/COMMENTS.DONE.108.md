# Critic 108: review of `dotnet/design/history/M22/P1-error-hierarchy-parity/PLAN.md` — resolved

Moved from `COMMENTS.108.md` after the plan revision; each finding ends with how and where it was resolved.

## 2. High: Phase 1 needs classes that only Phases 2 and 3 deliver

- **Section:** Phase 1 (PLAN:329-331), D5 (PLAN:142-144), D14 (PLAN:255-256), Phase 5 (PLAN:427).
- **Claim:** Phase 1 removes `IsRetriable` "including the soak client and the tests". The soak
  client's `ErrorIsRetriable` "becomes `ex is RetriableException`". With D14, "the soak client's
  checks [are] moved to classes". The exit is "No behaviour change other than D4, D5 and D14".
- **What is true:** `RetriableException`, `TopicExistsException`, `UnknownTopicOrPartitionException`
  and the authentication and authorization classes do not exist until Phase 2 generates them. The
  core does not build them until Phase 3 rewrites `FromFfiError`. Today only `KafkaException`,
  `SerializationException` and `RecordDeserializationException` exist (`ls src/Confluent.Kafka/Common/Errors/`).
  So after Phase 1:
  - the soak client cannot reference `RetriableException`;
  - if it could, `ex is RetriableException` would always be false, because the core still returns
    a plain `KafkaException` until Phase 3. This changes soak behaviour (no retriable error would
    ever retry);
  - the about 40 test files that assert `IsRetriable` have no class to assert instead.

  Phase 5 then repeats the soak change ("with D14 they are class checks"). D12 also requires the
  .NET and Python soak auth checks to change together, which Phase 1 does not do.
- **Corrected plan text:** "Phase 1 only adds the base constructor and the `Code` change (D4). The
  removal of `IsRetriable` (D5), the `Code` visibility change (D14) and the soak class checks move to
  a phase after Phase 3, when the classes exist and the core builds them. The soak changes are made
  once, in that phase, together with the matching Python soak change D12 requires."

Resolved: Phase 1 now only does the `Code`/core-constructor/`JavaText` work and keeps `IsRetriable`; D5 and the soak `RetriableException` change move to Phase 3, D14 and the soak tables to Phase 5. The suggested Python soak change is not needed: Python's soak reads `_ffi_id` and uses `RetriableError` (`python/soak/soakclient.py` `error_code`, `error_is_retriable`), so D12 drops it.

## 3. High: `PublicNamespaceLayoutTests` rejects the generated hierarchy, and no phase updates it

- **Section:** Phase 2 Exit (PLAN:361-362), D15 *Placement* (PLAN:282-284).
- **Claim:** "the generated classes compile in `dotnet build` and the existing .NET tests still
  pass." D15 mentions "amending §2 and the test", but no phase has that step.
- **What is true:** `tests/Confluent.Kafka.UnitTests/PublicNamespaceLayoutTests.cs` fails on the
  Phase 2 output in four ways:
  - `EveryExportedType_HasARow_AndEveryRowNamesAnExportedType` (:341-353) requires a hand-written row
    in `JavaCounterparts` for every exported type. About 165 generated classes have no row.
  - The same test fails on "Exported names below two namespaces" (`Duplicates(exported)`, :349).
    `NameBelowNamespace` (:474) strips the namespace, so the two `OffsetOutOfRangeException` and the
    two `InvalidOffsetException` classes (and, with D15, two `TimeoutException`s) collide. `Exported`
    (:478) uses `SingleOrDefault` on that name.
  - `NoPublicType_IsInTheRootNamespace` (:356-357) fails for every D15 root class.
  - `TheJavaClass_ExistsInTheKafkaSources` (:425-438) cannot find `java.lang.*` or `java.util.*`
    sources under `kafka/`.
- **Corrected plan text (Phase 2):** add a step: "Extend `PublicNamespaceLayoutTests`: key
  exported types by full name so that two Java classes of one simple name can both appear (as
  dotnet/CLAUDE.md §2 requires); take the rows of the generated error classes from the generator,
  or emit them; and, with D15, allow the root JDK classes and exempt them from the `kafka/` source
  lookup." Also amend dotnet/CLAUDE.md §2 in Phase 0.

Resolved: Phase 2 updates `PublicNamespaceLayoutTests` (full-name keys, generator-fed rows, JDK rule and source exemption, root exemption only under D19 (b)); D1 lists §2's amendment for Phase 0.

## 4. High: Phase 4 tests use C factories that do not exist, and the header is out of scope

- **Section:** Phase 4 Tests (PLAN:418-420), §4 (PLAN:305).
- **Claim:** "one test per class above, built from a core error through that class's C factory."
- **What is true:** the C functions that build a `kafka_common_Error_t` with fields are
  `kafka_common_Error_topic_authorization[_message]`, `_invalid_topics[_message]`,
  `_group_authorization[_with_message]`, `_throttling_quota_exceeded` and `_correlation_id_mismatch`
  (`grep -E "^kafka_common_Error_t \*" rust/target/include/confluent_kafka.h`). There is no
  factory that builds a core error for `ResourceNotFoundException`, `DuplicateResourceException`,
  `QuotaViolationException`, the consumer `OffsetOutOfRangeException`, `LogTruncationException`,
  `NoOffsetForPartitionException` or `RecordDeserializationException`. The `*Error_new` functions
  return the payload type, not a `kafka_common_Error_t`, and nothing converts one into the other.
  `kafka_common_Error_record_too_large(const char *message)` (header :19701) takes no partitions, so
  `RecordTooLargePartitions` cannot be filled. §4 forbids changing the header. Python tests these
  readers by replacing `KafkaError_payload` with a fake (`python/test/unit/test_errors.py:824`), not
  with real core errors.
- **Corrected plan text:** "Tests: for `TopicAuthorization`, `InvalidTopic`, `GroupAuthorization`,
  `ThrottlingQuotaExceeded` and `CorrelationIdMismatch`, a real core error built through its C
  factory. For the other classes, which have no C factory, test each reader against a fake view
  source behind an internal seam, as Python does, plus a broker-backed or mock test wherever the
  core raises the error. Each class's tests check each field and that the handle is freed exactly
  once." Alternatively, list the missing factories as a Rust-core dependency, which is Mode B.

Resolved: Phase 4 Tests uses the C factory for the five classes that have one, and a fake view source behind a seam plus mock or integration tests for the rest, as Python does; `record_too_large` (message only) is named. No header change (Mode A kept).

## 5. Medium: the class counts are wrong, and §2 contradicts D15

- **Section:** §2 (PLAN:62-66).
- **Claim:** "**174 classes**". "The 7 JDK classes (`Exception`, `RuntimeException` and the 5
  built-ins) keep mapping to .NET built-ins." The binding generates "**about 165**".
- **What is true:**
  - The graph has **175** classes: `python/test/unit/test_errors.py:235` asserts
    `len(_BY_JAVA) == 175`, and `ERROR-CATEGORIZATION.md:21,361` says the same.
  - Recomputed from `build_graph` (`rust/xtask/src/error_hierarchy.rs:858-1010`), the graph holds:
    the 8 `JDK_BUILTINS` (:637-695), the 161 non-`java.` BRIDGE rows (166 rows minus 5 `java.` ones;
    the C enum has 168 ids minus `NONE` and `LOCAL_CALLBACK`), the 5 `ABSTRACT_CLASSES` and
    `KafkaException`. That is 8 + 161 + 5 + 1 = 175.
  - `Exception` and `RuntimeException` are `TERMINAL_BASES`, not graph nodes. The JDK nodes are 8:
    the five with a C id plus `ExecutionException`, `NoSuchElementException` and
    `IndexOutOfBoundsException`.
  - The package list sums to the 167 Kafka classes. That part is correct.
  - "about 165" is correct only if no JDK class is generated (175 − 8 − 2). D15 recommends
    generating 6 JDK classes, which makes it 171, or 173 if all of `JDK_BUILTINS` is rendered as
    Phase 2's "from `JDK_BUILTINS`" says. That contradicts "keep mapping to .NET built-ins".
- **Corrected plan text:** "The graph has **175 classes**: 167 Kafka classes, including
  `KafkaException` and the 5 abstract bases, and the 8 JDK built-ins of `JDK_BUILTINS`. `Exception`
  and `RuntimeException` are terminal bases outside the graph. `KafkaException` and
  `RecordDeserializationException` stay hand-written (D8). With D15 the binding generates 165 Kafka
  classes plus the JDK classes D15 selects."

Resolved: §2's counts corrected to 175 / 167 Kafka (5 abstract) / 8 JDK, the terminal bases outside, 165 generated Kafka classes plus 6 JDK classes; the contradiction with D15 is gone.

## 6. Medium: D15 leaves out two of Python's JDK classes, and §7 understates the parent deviations

- **Section:** D15 (PLAN:261-279), §7 (PLAN:474-475).
- **Claim:** Python "gives each Java built-in exception the binding raises its own Java-named root
  class", and the plan lists six. Under §7, the only remaining parent deviation is
  "`ConcurrentModificationException` extends `InvalidOperationException`, because .NET has no
  `RuntimeException`".
- **What is true:**
  - Python generates eight root JDK classes. Besides the six, it has `NoSuchElementError(RuntimeError)`
    (`python/confluent_kafka/no_such_element_error.py`) and `IndexOutOfBoundsError(IndexError,
    RuntimeError)`. It raises them: `common/record/timestamp_type.py:51` and
    `admin/_mock_core.py:64`. The plan neither adds the two nor records their absence.
  - Every D15 class has a .NET parent instead of Java's: `IllegalStateException`,
    `IllegalArgumentException` and `UnsupportedOperationException` extend `RuntimeException` in Java
    (`JDK_BUILTINS`), and `java.util.concurrent.TimeoutException` extends `Exception`. Python keeps
    the Java chain through multiple inheritance (`IllegalArgumentError(ValueError, RuntimeError)`).
    `KafkaException` also extends `Exception`, not `RuntimeException`. All of these are the same
    deviation, not just `ConcurrentModificationException`.
- **Corrected plan text (§7):** "The JDK classes and `KafkaException` have the .NET built-in parent
  of the D15 table in place of Java's `RuntimeException` / `Exception`, because .NET has neither
  `RuntimeException` nor multiple inheritance." Then add `NoSuchElementException` and
  `IndexOutOfBoundsException` to D15, with the .NET bases `InvalidOperationException` and
  `ArgumentOutOfRangeException` (or whichever is chosen), or record their omission as a deviation.

Resolved: D15 names Python's two other JDK classes and offers generating them, recommending a recorded skip; §7's "Bases" item covers every D15 class and `KafkaException`.

## 7. Medium: D4's id-passing does not work through intermediate classes, so `RecordDeserializationException` gets −15

- **Section:** D4 (PLAN:120-127), D8 (PLAN:191-198), Phase 3 (PLAN:382).
- **Claim:** "each generated class passes its own id to a new `private protected` `KafkaException`
  constructor; abstract bases pass nothing, and their concrete subclasses supply the id."
- **What is true:** a C# constructor initializer can call only its direct base's constructors.
  `TopicExistsException : ApiException`, or `BufferExhaustedException : TimeoutException :
  RetriableException : ApiException`, cannot reach `KafkaException`'s constructor. Every generated
  class with subclasses would need its own `private protected` (or `protected`) constructor that
  passes a code on. That includes the concrete ones with their own id: `ApiException` (−6),
  `AuthorizationException`, `AuthenticationException`, `SerializationException` (−15), the
  `common.errors` `TimeoutException`, and others.
  The hand-written `RecordDeserializationException : SerializationException` calls
  `base(message, cause)` (`Common/Errors/RecordDeserializationException.cs:43`). Under D4 that is a
  public `SerializationException` constructor, which sets −15, but Python's id and the Phase 3
  hierarchy test want −27 (`python/.../record_deserialization_error.py:50`).
- **Corrected plan text (D4):** "Each generated class has a `private protected` constructor
  `(int code, …)` that forwards the code to its base, and its public constructors pass the class's
  own id through it. An abstract base has only that forwarding constructor plus its `protected` Java
  ones. The hand-written `RecordDeserializationException` passes −27 through
  `SerializationException`'s forwarding constructor."

Resolved with the per-class `virtual Code` override (D4 "Implementation (revised)"), Python's `_ffi_id` mechanism, so `RecordDeserializationException` reports −27 without a forwarding constructor chain.

Round 2: the overridden member is now the internal `FfiId`, with `Code` a non-virtual property returning it (D4, D14 (i), 117#14); under D14 (ii) it stays `Code`, its accessibility fixed in Phase 1.

## 8. Medium: Phase 1's new constructor has the same signature as the existing internal one, and about 40 test files depend on that one

- **Section:** Phase 1 (PLAN:326-329), D5 (PLAN:138-144).
- **Claim:** "Add `private protected KafkaException(int code, string? message, Exception?
  innerException)`" and remove "the `isRetriable` constructor parameters". The `IsRetriable` work
  touches "about 25 unit-test files".
- **What is true:**
  - `KafkaException.cs:76-86` has `internal KafkaException(int, string?, bool)` and
    `internal KafkaException(int, string?, bool, Exception?)`. Removing `bool` leaves
    `internal (int, string?, Exception?)`, the same signature as the new `private protected` one,
    which is CS0111. The plan does not say which survives.
  - A `private protected` constructor cannot be called by non-derived code. Today
    `NativeConsumer.cs:1001,1019`, `CreateTopicsResult.cs:269` and the tests call the internal one,
    for example `new KafkaException(36, "The operation failed.", isRetriable: false)` at
    `PublicNonGenericAsyncKafkaFutureTests.cs:316`. Under D4 those bare `KafkaException`s with a
    Kafka code are themselves wrong: they should be the class of the code.
  - `grep -rlE "IsRetriable|isRetriable" tests` gives **42 files**: 39 unit-test files and 3
    `GrpcServer.UnitTests` fixtures. `src/Confluent.Kafka/Internal/NativeConsumer.cs` also uses the
    parameter and is missing from D5's list.
- **Corrected plan text (Phase 1):** "Replace both internal constructors with one
  `private protected KafkaException(int code, string? message, Exception? innerException)`, plus a
  static internal factory for the binding's own bare `KafkaException`s (`WrapAtPoll`, `WrapAtClose`,
  fallback). Tests and binding code that build a coded `KafkaException` move to the class of the
  code. D5 touches 42 test files and `NativeConsumer.cs`."

Resolved: with the override no id constructor is added (D4); Phase 3 moves the binding's coded `KafkaException`s and the tests to their classes (D11) and then deletes the stored code and coded internal constructors; D5 lists `NativeConsumer.cs` and the 42 test files.

## 9. Medium: D3's internal core constructor collides with the translated public `(String, Throwable)` constructor

- **Section:** D3 (PLAN:113-114), D7 (PLAN:172-173).
- **Claim:** "Each class also gets one `internal` constructor that the core path uses." The registry
  builds "`(message, inner) => new X(…)`".
- **What is true:** many classes have a public Java `(String message, Throwable cause)`
  constructor (for example `InvalidTopicException.java:43` and `RecordTooLargeException.java:38`).
  D3 translates it to `(string?, Exception? innerException)`. An `internal (string?, Exception?)`
  core constructor then has the same signature (CS0111). The plan's clash check covers only pairs of
  Java constructors. The internal constructor's signature is never given.
- **Corrected plan text (D3):** "The `internal` core constructor has a signature no Java
  constructor can produce: a leading `CoreError` marker struct, then the message, the cause and,
  for a payload class, its fields. The clash check includes it."

Resolved: D3 "Implementation (revised)" gives the core-path constructor a leading `CoreError` marker, and the clash check includes it.

## 10. Medium: D3's type table is too small for Phase 2 to finish

- **Section:** D3 table (PLAN:104-109), Phase 2 *Generation errors* (PLAN:357-358).
- **Claim:** the table maps `String`, `Throwable`, `Set<String>` and `Map<TopicPartition, Long>`,
  and generation "stops on … a Java type with no C# mapping".
- **What is true:** the generated classes' public constructors also take `TopicPartition` and
  `Collection<TopicPartition>` (`NoOffsetForPartitionException.java:43,53`),
  `Map<TopicPartition, OffsetAndMetadata>` (`LogTruncationException.java:46`), `int`
  (`ThrottlingQuotaExceededException.java:33`, `CorrelationIdMismatchException.java:27`), `Object`
  (`ConfigException.java:35,39`, where `"Invalid value " + value` uses Java's `String.valueOf`), and
  `KafkaMetric` and `double` (`QuotaViolationException.java:34`). With the table as written,
  generation stops.
- **Corrected plan text (D3 table):** add `int` → `int`, `long` → `long`, `double` → `double`,
  `TopicPartition` → `TopicPartition`, `Collection<TopicPartition>` →
  `IReadOnlyCollection<TopicPartition>`, `Map<TopicPartition, OffsetAndMetadata>` →
  `IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>`, `Object` → `object?` (with the message
  formatting rule for Java's `String.valueOf`), and `KafkaMetric` → the D7 metric type.

Resolved: D3's table completed with `int`, `long`, `double`, `TopicPartition`, `Collection<TopicPartition>`, `Map<TopicPartition, OffsetAndMetadata>`, `Object` (with `JavaText.ValueOf`, D21), `KafkaMetric` (D7) and `InterruptedException` (D24).

## 13. Medium: `make verify` is not the .NET gate and runs no .NET test

- **Section:** §5 intro (PLAN:313), Phase 5 Exit (PLAN:429), Phase 6 Exit (PLAN:446).
- **Claim:** "Each phase ends green on `make verify` (`dotnet/CLAUDE.md` §7.5)". Phases 5 and 6 add
  "`make verify` green, including the multilanguage harness".
- **What is true:** dotnet/CLAUDE.md §7.5 names `make verify-dotnet` as the gate. The root
  `Makefile:354` defines `verify: build format-check check-generated lint test check-bindings`, and
  `test` is `test-rust-all-features test-c test-python` (:142). So `make verify` runs no .NET build,
  unit test, soak test or `__grpc_dotnet` arm. `verify-dotnet` (:373-375) does.
- **Corrected plan text:** "Each .NET phase ends green on `make verify-dotnet`
  (`dotnet/CLAUDE.md` §7.5), which includes the `__grpc_dotnet` / `__grpc_dotnet_async` arms.
  Phase 2 also ends green on `make verify` (for `check-generated`) plus `make verify-dotnet`."

Resolved: §5 now names `make verify-dotnet` as each .NET phase's gate, and Phase 2 also `make verify`; the Phase 5 and 6 Exits say `make verify-dotnet`.

## 14. Medium: Phase 3's "codes are unchanged" is wrong in three places

- **Section:** Phase 3 *Updated tests* (PLAN:390-391).
- **Claim:** "The cross-language harness keeps passing: codes are unchanged except
  `SerializationException`, now −15."
- **What is true:**
  - **−24 becomes −4 between Phases 3 and 5.** Phase 3 makes −24 a `CorrelationIdMismatchException
    : IllegalStateException : InvalidOperationException` (D6a), which is no longer a
    `KafkaException`. `Translate.ToProto` (`grpc-server/Translate.cs:116-125`) then reaches the
    `InvalidOperationException => -4` arm. Phase 5 fixes `ToProto`.
  - **`RecordDeserializationException` changes code.** It is built by the binding
    (`ConsumerRecordsMarshal.cs:290`) and reports 0 today. It becomes −15 or −27 (finding 7).
  - **Bare core `KafkaException`s change class.** A bare core `KafkaException` (C id −1, for example
    from `kafka_common_Error_kafka`) becomes whatever class the registry maps −1 to. Python maps −1
    to `UnknownServerError` (`python/test/unit/test_errors.py:840-842`). The plan never decides
    this, and it is not marked ⚠.
- **Corrected plan text:** "Codes on the wire change for `SerializationException` (−15),
  `RecordDeserializationException` (−27), and, until Phase 5 updates `ToProto`, −24 (sent as −4).
  Phase 3 updates `ToProto`'s `KafkaException` arm to keep −24, or moves that part of Phase 5 into
  Phase 3. The registry maps −1 to `UnknownServerException`, as Python does, and this change is
  listed with the ⚠ items."

Resolved: Phase 3 has a "Wire codes" bullet listing every change, and Phase 3 now updates `ToProto` by type (−24 included), so −24 never goes out as −4. One part of the claim is not right: Python does not map every −1 to `UnknownServerError`. `_build` gives a bare `KafkaError` when `is_api_error` is false; the cited test (`test_errors.py:840-842`) uses a fake library without `KafkaError_is_api_error`, so it reads as an API error. The plan follows Python's real rule (D18).

## 15. Low: D7 says C exposes less of the metric name than it does

- **Section:** D7 (PLAN:178-179).
- **Claim:** C "exposes only the metric's name and group, the value and the bound."
- **What is true:** `kafka_common_QuotaViolationError_metric_name` returns a full
  `kafka_common_MetricName_t`, and C has `kafka_common_MetricName_name`, `_group`, `_description` and
  `_tags` (`confluent_kafka.h:24660-24693`). Python reads only the name and the group
  (`_confluentkafka.c`, `py_KafkaError_payload`). What cannot be rebuilt is the `KafkaMetric`, not the
  `MetricName`.
- **Corrected plan text:** "C exposes the metric's full `MetricName` (name, group, description,
  tags), the value and the bound, but not the `KafkaMetric`. Python builds its metric from the name
  and group only; .NET matches Python, or reads the full `MetricName` and records that difference."

Resolved: D7's C sentence corrected (full `MetricName`, no `KafkaMetric`; Python reads two fields), and .NET reads all four.

## 16. Low: D1's list of rule passages to rewrite is incomplete

- **Section:** D1 (PLAN:79-86).
- **What is true:** these passages also state the flat model or the built-in mapping and are not
  listed:
  - **decision 17**, which D6(b) reverses: dotnet/CLAUDE.md:614, :1281 and ffi §B5's
    "Concurrent use" bullet;
  - dotnet/CLAUDE.md §3:352 and §4:733 (`GroupMetadata()` throws "the `INVALID_GROUP_ID` (24)
    `KafkaException`"), which D11 changes;
  - §4:914 ("a retriable `KafkaException`, code 7");
  - §3:579 (`SerializationException : KafkaException { } // flat subclass`);
  - §5:1080 (§A5 "flat `KafkaException` + preconditions");
  - ffi §B5's "Why" paragraph and "Tests required", which state `IsRetriable`;
  - ffi §B5's "local codes map by the error table" bullet.

  Also, §B5 has no "only code/retriable/fatal" sentence; it says "the header has no fatal
  predicate". That sentence is in §A5 only.
- **Corrected plan text:** add the passages above to D1's list, and attribute the
  "code/retriable/fatal" sentence to §A5 alone.

Resolved: D1's list now has decision 17's passages, the `GroupMetadata()` passages, the code-7 text, the `SerializationException` sketch, the §5 index line, ffi §B5's "Why", tests and local-codes bullet, and attributes the "code/retriable/fatal" sentence to §A5 alone.

## 17. Low: wrong header and incomplete P/Invoke list for the .NET tests

- **Section:** Phase 3 (PLAN:382, PLAN:385-386), D7 and Phase 4 (view names).
- **What is true:**
  - The .NET binding's ground truth is `confluent_kafka_bindings.h` (dotnet/CLAUDE.md §8.2), and
    `NativeAbiConformanceTests.cs:37` checks every import against it. In that header the payload
    views exist only as `_err` twins (for example `kafka_common_Error_topic_authorization_error_err`;
    the plain name has 0 matches). The enum is there too.
  - "the C factories `NativeMethods` declares (… the `local_*` ones)" leaves out
    `kafka_common_Error_local_unsupported_operation`. `NativeMethods` declares no binding for it
    (`grep EntryPoint = "kafka_common_Error_`), so a −31 test needs a new declaration.
- **Corrected plan text:** "read the ids from `rust/target/include/confluent_kafka_bindings.h`;
  declare the views' `_err` twins; add `kafka_common_Error_local_unsupported_operation_err` for the
  −31 test."

Resolved: Phase 3 reads the ids from `confluent_kafka_bindings.h` and declares `kafka_common_Error_local_unsupported_operation_err`; Phase 4 declares the views' `_err` twins.

## 18. Low: CA1724 is off in this repository

- **Section:** §6 Analyzers (PLAN:457).
- **Claim:** generated code may be flagged for "a namespace like `Common.Protocol.Types`
  (CA1724)".
- **What is true:** dotnet/CLAUDE.md §2 ("Names that are also namespaces") says "CA1724 stays at
  its default, off", and neither `.editorconfig` nor `Directory.Build.props` enables it.
- **Corrected plan text:** drop the CA1724 example, or say "CA1724 is off (§2); if it is enabled
  later, …".

Resolved: §6 Analyzers drops the CA1724 example and says CA1724 is off (§2).

## 19. Low: Phase 3 deletes `FromHandle` without listing the tests that call it

- **Section:** Phase 3 *One reader* and *Updated tests* (PLAN:371-377, PLAN:389).
- **What is true:** besides `KafkaExceptionFfiErrorTests`, these tests call `FromHandle` /
  `FromBorrowedHandle` and stop compiling when the old reader goes:
  - `KafkaExceptionReadTests.cs` (:56, :75, :88-89, :122, :138-139);
  - `KafkaExceptionTests.cs:73`;
  - `KafkaExceptionCauseTests.cs:68`;
  - `Interop/ManagedExceptionCarrierTests.cs:151-163`.
- **Corrected plan text:** add these four files to Phase 3's *Updated tests*, moved to
  `FromFfiError(…, owned: true/false)`.

Resolved: Phase 3 "Updated tests" lists `KafkaExceptionReadTests.cs`, `KafkaExceptionTests.cs`, `KafkaExceptionCauseTests.cs` and `Interop/ManagedExceptionCarrierTests.cs`.

# Round 2 (verification of the revised plan)

## 12. Medium: D14 misses most of the soak client's code reads

- **Section:** D14 (PLAN:254-256).
- **Claim:** the gRPC server keeps reading `Code` through `InternalsVisibleTo`. "the soak client's
  code checks (`TopicAlreadyExistsCode`, `UnknownTopicOrPartitionCode` and the D12 auth list)
  become class checks."
- **What is true:**
  - `dotnet/soak/SoakClient/SoakClient.cs` also classifies by code through `s_coordinatorErrorCodes`
    {14, 15, 16} (:58-63) and `s_disconnectErrorCodes` {5, 6, 7, 8, 13} (:65-72), all through
    `ErrorCode(Exception)` (:306, used at :743, :1091, :1104, :1200, :1309).
  - The soak tests assert `error.Code` (`SoakClient.Tests/SoakTestSupport.cs:71`,
    `TopicSetupTests.cs:175,318`).
  - The library's `InternalsVisibleTo` lists only `Confluent.Kafka.UnitTests` and
    `Confluent.Kafka.GrpcServer.UnitTests` (`Confluent.Kafka.csproj:94-95`), not the gRPC server and
    not soak.
  - Python's soak reads `_ffi_id` for its classification (`python/soak/soakclient.py:378`). The
    Python counterpart of the .NET soak reading `Code` is therefore a soak `InternalsVisibleTo`, not
    class checks.
- **Corrected plan text (D14):** "Add `InternalsVisibleTo` for `Confluent.Kafka.GrpcServer` and the
  soak client and its tests, which read `Code` as Python's soak reads `_ffi_id`. Or, if soak moves
  to classes, the coordinator and disconnect sets move too, and so do the soak tests' `Code`
  assertions."

Resolved: D14 grants `InternalsVisibleTo` to the gRPC server, soak and its tests; D12 keeps every soak table (coordinator and disconnect included) on the internal `Code`, and the soak tests' `Code` assertions keep compiling.

**Reopened (verification round):** the fix names the wrong friend assemblies. D14 (PLAN:377) grants `InternalsVisibleTo` to `Confluent.Kafka.Soak` and "the soak tests", but a friend grant names the *assembly*, and `Confluent.Kafka.Soak` is only the soak client's `RootNamespace`: its `AssemblyName` is `SoakClient` (`soak/SoakClient/SoakClient.csproj:33-34`) and the tests' is `SoakClient.Tests` (`soak/SoakClient.Tests/SoakClient.Tests.csproj:23-24`). The existing grants name assemblies (`src/Confluent.Kafka/Confluent.Kafka.csproj:94-95`). As written, Phase 5 makes `Code` internal and the soak client's code tables (`ErrorCode`) and the soak tests' `Code` assertions stop compiling. The gRPC server's name is right (`grpc-server/Confluent.Kafka.GrpcServer.csproj:39`). Corrected D14 text: "`InternalsVisibleTo` for the assemblies `Confluent.Kafka.GrpcServer`, `SoakClient` and `SoakClient.Tests` (each with the shared public key)".

Resolved (round 2, with 116#20): D14 and Phase 5 name the assemblies `Confluent.Kafka.GrpcServer`, `SoakClient` and `SoakClient.Tests`, each with the shared public key.

## 20. Medium: Phase 3's "unknown-server API error" test cannot be built through the C factories

- **Section:** Phase 3, "New `FromFfiErrorClassTests`" (PLAN:685-689).
- **Claim:** "real core errors built through the C factories give the right class:
  `kafka_common_Error_kafka_err` gives exactly `KafkaException` (D18), an unknown-server API error
  gives `UnknownServerException`, …"
- **What is true:**
  - None of the owned-error factories in BH builds an error with id −1 for which
    `kafka_common_Error_is_api_error_err` is true. The full list of `kafka_common_Error_t *`
    factories is `buffer_exhausted`, `config_*`, `correlation_id_mismatch`, `group_authorization*`,
    `invalid_group_id`, `invalid_receive`, `invalid_topics*`, `kafka*`, `local_*`,
    `record_batch_too_large`, `record_too_large`, `schema*`, `serialization`,
    `throttling_quota_exceeded`, `timeout`, `topic_authorization*`, `transaction_aborted*`,
    `unsupported_version` and `wakeup` (`grep -nE "^kafka_common_Error_t \*" rust/target/include/confluent_kafka_bindings.h`).
    There is no code-to-error entry either (no `Errors_for_code` or similar in BH).
  - `kafka_common_Error_kafka_err` gives −1 with `is_api_error` false, which is the other half of D18.
  - Python tests this branch with a fake native library whose `KafkaError_is_api_error` it
    toggles (`python/test/unit/test_producer_family.py:3985-4013`), not with a real core error.
- **Corrected plan text (Phase 3):** "an unknown-server API error gives `UnknownServerException`,
  tested through an internal seam that feeds `FromFfiError`'s classification a code and an
  `is_api_error` value (Python fakes `KafkaError_is_api_error` the same way), or through a
  mock/broker path that reports `UNKNOWN_SERVER_ERROR`; no C factory builds one."

Resolved (with 117#17): Phase 3 tests the API branch through an internal seam that gives `FromFfiError`'s classification an id and an `is_api_error` value, as Python fakes `KafkaError_is_api_error`, and says no C factory builds one; the bare branch is tested with `kafka_common_Error_kafka_err`.

## 21. Medium: Phase 4 cannot switch the generated registry to the payload readers

- **Section:** Phase 2 "Registry" (PLAN:626-630), Phase 4, §5 modes (PLAN:574-576).
- **Claim:** "Classes with fields point at the Phase 4 readers; until Phase 4 lands, those entries
  use the core-path constructor with the message only."
- **What is true:**
  - `Internal/ErrorClasses.g.cs` is generated by the Rust xtask, and "`check-generated` diffs the
    C# output" (PLAN:631). So the registry entries change only when the generator changes.
  - Phase 4 is Mode A (`dotnet-actor` / `dotnet-critic`, PLAN:574), and the `dotnet-actor` does not
    author Rust. Phase 4's bullets touch only `ErrorPayloadMarshal` and `NativeMethods`; none
    changes the generator.
  - So either the Phase 2 output already calls reader methods that do not exist yet (Phase 2 does
    not build), or Phase 4 has to edit generated code (check-generated fails), or Phase 4 needs an
    xtask change that no phase lists, under personas Phase 4 does not use.
- **Corrected plan text (Phase 2, Registry):** "For a class with fields the generated entry calls a
  fixed reader, `ErrorPayloadMarshal.Read<Class>(message, inner, error)`. Phase 2 adds a
  hand-written `ErrorPayloadMarshal` whose readers build the class with the message only, so the
  registry compiles; Phase 4 replaces their bodies with the view reads, and the generator does not
  change again."

Resolved: Phase 2's Registry bullet has each class with fields call a fixed `ErrorPayloadMarshal.Read<Class>(message, inner, error)`, whose Phase 2 body builds the class from the message only; Phase 4 replaces the bodies, changes no generated file and stays Mode A. D7 says the same.

## 22. Medium: §6 says every visible change is marked ⚠, but D11 and D22 are not, and D6(a) contradicts "`catch (KafkaException)` still catches every Kafka class"

- **Section:** §6 "Behaviour changes users can see" (PLAN:801-803), D6, D11, D22.
- **Claim:** "All are listed as ⚠ in §3 (D4, D5, D6, D9, D13, D14, D15, D18, D20, D23). Adding the
  classes is otherwise non-breaking: `catch (KafkaException)` still catches every Kafka class".
- **What is true:**
  - D6(a) says itself that making `CorrelationIdMismatchException` an `IllegalStateException`
    "takes the class out of `catch (KafkaException)`, which catches it today" (PLAN:227-228). That
    class is Java's `org.apache.kafka.common.requests.CorrelationIdMismatchException`, a Kafka
    class. So the §6 sentence is false under D6's own recommendation.
  - D11 (a) changes the 15 invariant errors (`grep -rn 'new KafkaException("' dotnet/src` gives
    9 "produced no …" sites and 6 others in `DelegationTokenMarshal.cs`, `LogDirMarshal.cs` and
    `FeatureMetadataMarshal.cs`). Some stop throwing and return `null` or an empty value, and the
    rest become `InvalidOperationException`, which `catch (KafkaException)` no longer catches. D6(a)
    is marked ⚠ for exactly this effect, but D11 has no ⚠ (PLAN:316) and is not in the §6 list.
  - D22 (a) changes `Message` from .NET's "Exception of type '…' was thrown." to `""` for every
    Java `null` message (no-argument constructors, a timed-out `Get`). The gRPC server sends that
    text. D22 has no ⚠ and is not in the list.
- **Corrected plan text:** mark D11 and D22 with ⚠, add them to the §6 list, and replace the
  sentence with: "`catch (KafkaException)` still catches every Kafka class except
  `CorrelationIdMismatchException` (D6), and no longer catches the binding's invariant errors
  (D11)."

Resolved (with 116#19 and 117#20): D11, D21 and D22 are marked ⚠; §6 lists D4, D5, D6, D9, D11, D13, D14, D15, D18, D20, D21, D22 and D23, and its sentence now reads that `catch (KafkaException)` still catches every Kafka class except `CorrelationIdMismatchException` (D6) and no longer catches the binding's invariant errors (D11).

## 23. Low: the internal type → id lookup leaves out `CorrelationIdMismatchException`, which has no `Code` either

- **Section:** Phase 2 "Registry" (PLAN:627-628), Phase 3 `ErrorHierarchyTests` (PLAN:682-683),
  Phase 5 (PLAN:754-757), D12 (PLAN:345-347).
- **Claim:** the lookup covers "the classes that are not `KafkaException`s (the D15 classes)";
  Phase 5: "the gRPC server reads `Code` for a `KafkaException` and the registry's type → id
  lookup for the rest"; D12: soak's `ErrorCode` "reads the id of every class that has one, the D15
  classes included, through the generated id lookup"; Phase 3: "each class's `Code` matches the C
  enum".
- **What is true:** under D6(a), `CorrelationIdMismatchException` derives from the D15
  `IllegalStateException`, so it is not a `KafkaException` and has no `Code` (D4 puts `Code` on
  `KafkaException`). It has its own C id, −24 (`kafka_common_Error_CORRELATION_ID_MISMATCH`, BH:368).
  It is not a D15 class (D15's table, PLAN:394-401), so a lookup scoped to "the D15 classes" has no
  entry for it. Then the Phase 5 server and D12's `ErrorCode` both lose −24, and the Phase 3 test
  has no `Code` to compare. Phase 3's `ToProto` sends −24 through a type arm of its own
  (PLAN:674-675), so the gap appears only once Phase 5 moves the server to the lookup.
- **Corrected plan text (Phase 2):** "an internal type → id lookup for every class with a C id that
  does not derive from `KafkaException` (the D15 classes and `CorrelationIdMismatchException`)";
  and in Phase 3: "each class's id (its `Code`, or the lookup for a class without one) matches the
  C enum".

Resolved with 107#13: the lookup covers `CorrelationIdMismatchException`, and `ErrorHierarchyTests` checks each class's id through it.

## 24. Low: D11's Python citation leaves out the function that substitutes `""` for a NULL transactional id

- **Section:** D11 (PLAN:324-327).
- **Claim:** Python substitutes "`""` for a NULL transactional id (`python/_confluentkafka.c`,
  `delegation_token_to_py`, `conv_feature_metadata`, `conv_transaction_description`,
  `conv_partition_producer_state`)".
- **What is true:** the four functions cited do the `None` and `[]` substitutions
  (`_confluentkafka.c:8033`, `:8554`, `:8584`, `:8576`). The `""` for a NULL transactional id is in
  `transaction_listing_to_py` (`_confluentkafka.c:8128-8140`, `tid ? tid : ""`). Phase 3 "checks
  each of the 15 sites against the Python function that handles the same gap" (PLAN:335-336), and
  `TransactionMarshal.cs:74` has no Python counterpart among the cited functions.
- **Corrected plan text:** add `transaction_listing_to_py` to the list of cited functions.

Resolved: D11 now cites `transaction_listing_to_py` for the `""` substituted for a NULL transactional id.
