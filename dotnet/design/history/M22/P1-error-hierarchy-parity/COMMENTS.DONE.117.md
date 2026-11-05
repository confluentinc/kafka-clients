# Critic 117 — review of `dotnet/design/history/M22/P1-error-hierarchy-parity/PLAN.md` — resolved

Moved from `COMMENTS.117.md` after the plan revision; each finding ends with how and where it was resolved.

Verification round: #2, #3, #4 and #7 to #13 are confirmed against the revised plan. #5 is moved
back to `COMMENTS.117.md`, because a `String` operand of a Java `+` and the elements of a
collection or map are still not formatted through `String.valueOf`.
#16 there is a new finding on the timeout message D13 now states, not a reopening of #7.

## 2. [Medium] Phase 2 cannot reuse the constructor `Effect`s: they are Python text, flattened over the chain

- **Plan section:** Phase 2 "Renderer" ("reuses `build_graph`, `Graph` and the constructor
  `Effect`s").
- **Problem:** `Effect` is the Python translation, not a language-neutral model:
  - Every value is Python source text.
  - An `Effect` flattens the whole constructor chain into one message, one cause and the fields of
    every class in the chain.

  C# can't use either. `Exception.Message` and `InnerException` can be set only by passing them
  down the base constructors, and a subclass can't assign its base's private fields. For example,
  `org.apache.kafka.clients.consumer.LogTruncationException#LogTruncationException(String,Map,Map)`
  sets `offsetOutOfRangePartitions` by calling `OffsetOutOfRangeException(String, Map)`.

  C# has `: this(...)` / `: base(...)` like Java, so the natural C# renderer translates each Java
  constructor one level at a time from the parsed `java_parse::Ctor` (`call` plus `assigns`), with
  its own C# expression translator. As written, the plan understates Phase 2 and points the Actor
  at a design that doesn't fit C#.

  One related constraint is missing: the generated `.cs` files sit in the solution that
  `dotnet format --verify-no-changes` checks (`dotnet/Makefile:157`), and they don't have a
  generated-code name or header. They must come out already format-clean, or `dotnet format` and
  `check-generated` will disagree about the same files.
- **Evidence:** in `rust/xtask/src/error_hierarchy.rs`:
  - `PyVal` and `Effect` (1267-1299): "A Python value … its source text"; "the fields it sets
    (Python attribute name -> value), over the whole chain".
  - `Graph::translate` (1506-1625) emits `"None"`, `"True"`, `"set()"`,
    `_throwable.checked_set(...)` and `java_str(...)`, and turns `new InterruptedException()` into
    `None` (1582-1586).
  - `throwable_effect` (1745) emits `_throwable.cause_message(...)`, and `convert_for_field` (1762)
    emits `_throwable.copy_set/copy_dict/view/headers`.
  - `ClassInfo` (732-758) carries `py_name`, `py_module` and `unserializable`.
  - `generated_outputs` (3372), `orphaned_outputs` (3494, which walks `python/confluent_kafka` for
    `.py`/`.pyi` only) and `unlisted_packages` (3526, which reads `python/pyproject.toml`) are
    Python-only.
- **Suggested fix:** Replace the Renderer's first sentence with:

  "The C# renderer reuses `build_graph`, `Graph`, the `BRIDGE` and `JDK_BUILTINS` tables and the
  parsed Java classes (`java_parse::JavaClass`: constructors, fields, getters, singletons,
  factories). It does not reuse `Graph::eval` or `Effect`. Instead it translates each Java
  constructor into one C# constructor with the same `this`/`super` call and field assignments,
  through a C# expression translator that is the counterpart of `Graph::translate`. The Python
  output stays byte-identical under `check-generated`, and the C# output is `dotnet format`-clean."

  Size Phase 2 for a second translator, not a renderer over existing output.

Resolved: Phase 2's Renderer bullet now reuses `build_graph`, `Graph`, `BRIDGE`, `JDK_BUILTINS` and `java_parse::JavaClass`, not `Graph::eval`/`Effect`, translates each constructor one level at a time through a C# expression translator, is sized as a second translator, keeps the Python output byte-identical, emits `dotnet format`-clean files, and gives the Python-only output/orphan/package functions C# counterparts.

## 3. [Medium] D4/D3 constructor mechanics don't compile as described

- **Plan section:** D4 ("each generated class passes its own id to a new `private protected`
  `KafkaException` constructor"), D3 ("one `internal` constructor that the core path uses"),
  Phase 1, D8. D4's meaning is not reopened, only its mechanism.
- **Problem:**
  1. A C# constructor initializer can call only the direct base class's constructors. So only
     direct subclasses of `KafkaException` (`ApiException`, `SerializationException`,
     `ConfigException`, …) can pass it an id. `TopicExistsException : ApiException` reaches only
     `ApiException`'s constructors, which pass −6.
     - To make D4 work, every class that has subclasses needs an id-taking twin of each
       constructor its subclasses call, including the payload ones: `LogTruncationException` (−20)
       chains through `OffsetOutOfRangeException(String, Map)` (−22).
     - The hand-written `RecordDeserializationException : SerializationException` calls
       `: base(message, cause)` (`dotnet/src/Confluent.Kafka/Common/Errors/RecordDeserializationException.cs:43`).
       It can report −27 only through such a twin on the generated `SerializationException`.
  2. The core-path constructor can't be `(string? message, Exception? innerException)`, which is
     the shape D7's `(message, inner) => new X(…)` suggests. At least 96 concrete generated classes
     already get that signature from Java's public `(String, Throwable)` constructor, and CS0111
     ignores accessibility. D3's clash rule covers only pairs of Java constructors.
  3. Once D5 removes `isRetriable`, today's
     `internal KafkaException(int code, string? message, bool isRetriable, Exception? innerException)`
     (`dotnet/src/Confluent.Kafka/Common/KafkaException.cs:81-86`) becomes `(int, string?, Exception?)`.
     That is the same signature as Phase 1's new `private protected` constructor.
- **Evidence:**
  - C# language specification, constructor initializers: `base(argument-list)` invokes "an instance
    constructor from the direct base class".
  - Java chains: `org.apache.kafka.clients.consumer.LogTruncationException#LogTruncationException(String,Map,Map)`
    calls `super(message, fetchOffsets)`.
  - 99 generated classes declare `public X(String, Throwable)`. Three of them are abstract
    (`RetriableException`, `RefreshRetriableException`, `ApplicationRecoverableException`).
- **Suggested fix:**
  - Keep D4's meaning but take Python's mechanism: the id is a member of the class, not a
    constructor argument, as `_ffi_id` is a class attribute.
    - `KafkaException` declares `virtual int Code => -1` (internal if D14 is accepted).
    - Each generated class with an id overrides it; abstract bases don't.
    - The hand-written `RecordDeserializationException` overrides it with −27.
  - Phase 1 then adds no id constructor; it drops `isRetriable` from the existing internal
    constructors or removes them.
  - For the core path, use the class's own Java `(String, Throwable)` constructor where one exists.
    Emit an internal `(string?, Exception?)` constructor only where Java has none.
  - Extend Phase 2's clash check to every constructor emitted, internal ones included.

Resolved: D4 "Implementation (revised)" takes the per-class `virtual Code` override (no id constructor; `RecordDeserializationException` overrides −27); D3 "Implementation (revised)" gives the marker-typed core-path constructor; Phase 3 deletes the stored code and coded internal constructors once D11 moves their users, so no CS0111 with the old `(int, string?, Exception?)`.

Round 2: the overridden member is now the internal `FfiId`, with `Code` a non-virtual property returning it (D4, D14 (i), 117#14); under D14 (ii) it stays `Code`, its accessibility fixed in Phase 1.

## 4. [Medium] D3's type table is incomplete: generation would stop at nine classes

- **Plan section:** D3 (the table), Phase 2 "Generation errors" ("a Java type with no C# mapping"),
  D7, §7.
- **Problem:** The table maps `String`, `Throwable`, `Set<String>` and `Map<TopicPartition, Long>`.
  The generated classes' public constructors and getters also use `Object`, `int`, `double`,
  `TopicPartition`, `Collection<TopicPartition>`, `Set<TopicPartition>`,
  `Map<TopicPartition, OffsetAndMetadata>`, `KafkaMetric` and `InterruptedException`. Under
  Phase 2's rule, generation stops at:
  - `org.apache.kafka.common.config.ConfigException`: `(String, Object)` and
    `(String, Object, String)`.
  - `org.apache.kafka.common.errors.InterruptException`: `(InterruptedException)`,
    `(String, InterruptedException)`, and `(String)`, whose body is
    `super(message, new InterruptedException())`.
  - `org.apache.kafka.common.metrics.QuotaViolationException`: `(KafkaMetric, double, double)`,
    plus `metric()`, `value()` and `bound()`.
  - `org.apache.kafka.clients.consumer.LogTruncationException`: the constructors and
    `divergentOffsets()` use `Map<TopicPartition, OffsetAndMetadata>`.
  - `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`: `(TopicPartition)`,
    `(Collection<TopicPartition>)`, and `partitions()`, which returns `Set<TopicPartition>`.
  - `org.apache.kafka.clients.consumer.InvalidOffsetException#partitions()` and
    `org.apache.kafka.clients.consumer.OffsetOutOfRangeException#partitions()`: `Set<TopicPartition>`.
  - `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`: `(int, String)` and
    `throttleTimeMs()`.
  - `org.apache.kafka.common.requests.CorrelationIdMismatchException`: `(String, int, int)` and its
    two `int` getters.

  Two of these need a decision, not just a row:
  - **`KafkaMetric`** has no .NET type. .NET's `IMetric` is Java's `Metric` interface
    (`PublicNamespaceLayoutTests` row `("IMetric", "org.apache.kafka.common.Metric")`). D7 hands back
    an `IMetric`, but nothing records the narrowing of the public constructor and getter.
  - **`InterruptedException`** has no .NET class. Every `InterruptException` constructor also runs
    `Thread.currentThread().interrupt()`, which `java_parse.rs:594` drops silently. Python
    records both choices: `new InterruptedException()` leaves the cause empty
    (`error_hierarchy.rs:1582-1586`).
- **Evidence:** the Java members above. Python's type map, `py_type`
  (`rust/xtask/src/error_hierarchy.rs:2256-2292`), has a row for every one of these types.
- **Suggested fix:**
  - Complete D3's table for constructor parameters and getter returns:
    - `int` → `int`
    - `double` → `double`
    - `Object` → `object?`
    - `TopicPartition` → `TopicPartition`
    - `Collection<TopicPartition>` and `Set<TopicPartition>` → `IReadOnlyCollection<TopicPartition>`
    - `Map<TopicPartition, OffsetAndMetadata>` → `IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>`
    - `KafkaMetric` → `IMetric`
    - `InterruptedException` → a chosen type, for example `System.Threading.ThreadInterruptedException`,
      with `new InterruptedException()` translated to match.
  - Add to §7: the `KafkaMetric` → `IMetric` narrowing, and that `InterruptException` does not
    re-interrupt the thread.

Resolved: D3's table is complete for the listed types. `KafkaMetric` is decided in D7 (options, recorded in §7), `InterruptedException` in the new D24 (options; recommendation as Python, the missing re-interrupt recorded in §7).

## 7. [Medium] D13: a timed-out `Get` should throw D15's `TimeoutException`, and "toString() form" needs defining

- **Plan section:** D13, D15, §7.
- **Problem:**
  - **(a) The timeout class.** D13 says "Unchanged, as in Java: a timeout still throws
    `System.TimeoutException`". Java's `get(long, TimeUnit)` throws
    `java.util.concurrent.TimeoutException`, and Python's timed-out `get` raises the Java-named
    root `confluent_kafka.TimeoutError`. With D15 that class exists in .NET
    (`TimeoutException : System.TimeoutException`). Throwing the plain built-in therefore keeps a
    deviation from both Java and Python, and §7 doesn't list it. Throwing the D15 class keeps every
    `catch (System.TimeoutException)` working.
  - **(b) The message.** "Its message is the error's `toString()` form" is ambiguous in .NET, where
    `Exception.ToString()` includes inner exceptions and the stack trace. Java's
    `ExecutionException(Throwable)` uses `Throwable.toString()`: the class name, then `": "` and
    the message, or the class name alone when the message is null.
- **Evidence:**
  - Java:
    - `org.apache.kafka.common.internals.KafkaFutureImpl#get(long,TimeUnit)` (declares
      `java.util.concurrent.TimeoutException`, thrown by `CompletableFuture#get(long,TimeUnit)`).
    - `org.apache.kafka.clients.producer.internals.FutureRecordMetadata#get(long,TimeUnit)`
      (`throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.")`, which
      imports `java.util.concurrent.TimeoutException`).
  - Python: `python/confluent_kafka/common/kafka_future.py` `_timed_out()`, documented as "Java's
    `java.util.concurrent.TimeoutException` of a timed-out `get`".
  - .NET: `dotnet/src/Confluent.Kafka/Common/KafkaException.cs` `CauseMessage`
    (`cause.GetType().FullName + ": " + cause.Message`).
- **Suggested fix:** In D13:
  - "A timeout throws the Java-named `TimeoutException` (D15), which is a `System.TimeoutException`,
    as Java's `get(long, TimeUnit)` and Python's `get(timeout=…)` do."
  - "Its message is Java's `Throwable.toString()` of the error (the class's full name, then `": "`
    and the message), built by the same helper as `KafkaException(Exception cause)`, not .NET's
    `Exception.ToString()`."

  Phase 6's translated `KafkaFutureTest` cases then assert the timeout class as well.

Resolved: D13 now throws the D15 `TimeoutException` on timeout and defines the message as Java's `Throwable.toString()` built by D21's helper, not .NET's `Exception.ToString()`; Phase 6 tests the timeout class.

## 8. [Medium] `PublicNamespaceLayoutTests` must change before Phase 2 can exit with "the existing .NET tests still pass"

- **Plan section:** Phase 2 Exit, D15, the same-name rule in §6 and `dotnet/CLAUDE.md` §2.
- **Problem:** The test keeps a hand-written `JavaCounterparts` table, keyed by each type's name
  below its namespace. It fails on:
  - **(a) Exported types without a row.** Phase 2 adds about 165 generated classes, plus 6 more
    with D15.
  - **(b) Two exported types with the same name below their namespaces** ("Exported names below two
    namespaces"):
    - `Consumer.OffsetOutOfRangeException` and `Common.Errors.OffsetOutOfRangeException`;
    - the two `InvalidOffsetException`s;
    - with D15, two `TimeoutException`s.

    This is exactly the layout `dotnet/CLAUDE.md` §2 prescribes. The test's `Exported(name)` also
    uses `SingleOrDefault`, which throws on such a pair.
  - **(c) D15 rows that name a `java.*` class.** They fail
    `Assert.StartsWith("org.apache.kafka.", …)` in `TheNamespace_IsTheJavaPackages_OrThePairedTypes`,
    and `TheJavaClass_ExistsInTheKafkaSources` finds no source file for them under `kafka/`.
- **Evidence:** in `dotnet/tests/Confluent.Kafka.UnitTests/PublicNamespaceLayoutTests.cs`:
  `EveryExportedType_HasARow_AndEveryRowNamesAnExportedType` (`Duplicates(exported)`),
  `NameBelowNamespace`, `Exported` (`SingleOrDefault`), `JavaPrefix` and
  `TheJavaClass_ExistsInTheKafkaSources`. The duplicate names come from
  `python/confluent_kafka/_error_registry.py`: `InvalidOffsetException`, `OffsetOutOfRangeException`
  and `TimeoutException`, twice each.
- **Suggested fix:** Phase 2 updates the test in the same change:
  - Key the rows by namespace-qualified name, and drop the "names below two namespaces" check,
    which §2's same-name rule contradicts.
  - Take the error rows from the generator (an emitted row list, or the registry) instead of
    hand-adding about 170 rows.
  - Give the `java.*` classes their own namespace rule and source check, or exclude them.

  Change the Exit to "the existing tests pass, with `PublicNamespaceLayoutTests` updated as above".

Resolved: Phase 2 updates `PublicNamespaceLayoutTests` (full-name keys, drop the duplicate-name check, generator-fed rows, a JDK rule and source check), and its Exit says so.

## 9. [Low] §2's class counts are off, and the JDK sentence contradicts D15

- **Plan section:** §2 ("174 classes"; "The 7 JDK classes (`Exception`, `RuntimeException` and the
  5 built-ins) keep mapping to .NET built-ins"), D15.
- **Problem:**
  - The graph has 175 classes: 167 Kafka classes, 5 of them abstract, and the 8 `JDK_BUILTINS`.
    `Exception`, `RuntimeException` and `Throwable` are terminal bases, not classes in the graph.
  - "Keep mapping to .NET built-ins" contradicts D13 and D15, which generate 6 Java-named JDK
    classes.
  - D15 doesn't say why the other two classes Python generates, `NoSuchElementException` and
    `IndexOutOfBoundsException`, are left out. The renderer needs that as a skip rule, because
    `JDK_BUILTINS` has all 8.
- **Evidence:**
  - `python/confluent_kafka/_error_registry.py` `ERRORS` has 175 rows, one per graph class
    (`generated_outputs`, `rust/xtask/src/error_hierarchy.rs:3442-3447`).
  - `JDK_BUILTINS` (637-702) has 8 entries; `TERMINAL_BASES` is at 706-710.
  - `ERROR-CATEGORIZATION.md` §5.1 says "There are 175 classes".
  - Java's one public throw of `NoSuchElementException` is
    `org.apache.kafka.common.record.TimestampType#forName(String)`. .NET's `TimestampType` is an
    enum without it.
- **Suggested fix:** "The graph has 175 classes: 167 Kafka classes (5 abstract) and 8 JDK classes.
  The binding generates 165 Kafka classes (`KafkaException` and `RecordDeserializationException`
  stay hand-written, D8) and, with D15, 6 JDK classes. `NoSuchElementException` and
  `IndexOutOfBoundsException` are not generated because .NET throws neither; they are added when a
  translated member throws one (for example `TimestampType.forName`)."

Resolved: §2 now says 175 classes (167 Kafka with 5 abstract, 8 JDK; the terminal bases outside), 165 generated Kafka classes, and 6 of the 8 JDK classes with D15; D15 gives the skip rule for `NoSuchElementException` and `IndexOutOfBoundsException`, recorded in §7.

## 10. [Low] The two `INSTANCE` singletons are missing

- **Plan section:** D3, Phase 2 Renderer, §7.
- **Problem:** Java exposes `DisconnectException.INSTANCE` and
  `CoordinatorNotAvailableException.INSTANCE`; the second is built with a private constructor.
  Python generates both. The renderer list doesn't mention them, and D3's "each public Java
  constructor" wouldn't emit the private constructor they need. As written, dropping them is a
  deviation from both Java and Python that the plan doesn't record.
- **Evidence:**
  - Java: `org.apache.kafka.common.errors.DisconnectException#INSTANCE`, and
    `org.apache.kafka.common.errors.CoordinatorNotAvailableException#INSTANCE` with its private
    `CoordinatorNotAvailableException()`.
  - Python: `python/confluent_kafka/common/errors/disconnect_error.py:60` and
    `coordinator_not_available_error.py:61` (`_throwable.singleton`); the root `CLAUDE.md` Python
    conventions name `DisconnectError.INSTANCE`.
  - The parser already collects them in `java_parse::JavaClass.singletons`.
- **Suggested fix:** Add to Phase 2: "a `public static readonly` field (`Instance`) for each Java
  `public static final` instance, built with the constructor Java names; a private Java
  constructor used for it is emitted `private`." Add a sentence to its doc: .NET records a stack
  trace on every `throw`, while Java's `ApiException#fillInStackTrace` records none, so the shared
  instance is for comparing and catching, not for throwing from several threads.

Resolved: D3 "Implementation (revised)" adds the singletons as `public static readonly X Instance`, a private Java constructor emitted `private`, with the stack-trace note in its doc; Phase 2 tests a singleton; §7 lists `Instance` for `INSTANCE`.

## 11. [Low] `QuotaViolationException`'s text: D7 understates what C exposes, and D16 misses the wrapping paths

- **Plan section:** D7 ("C … exposes only the metric's name and group, the value and the bound"),
  D16.
- **Problem:**
  1. **D7's claim isn't accurate.** `kafka_common_QuotaViolationError_metric_name` returns a full
     `kafka_common_MetricName_t`, which has `name`, `group`, `description` and `tags` accessors, and
     the core keeps the whole `MetricName`. Python reads only the name and group, which is a gap in
     Python, not in C. Java's `toString()` prints the whole `metric.metricName()`
     (`MetricName [name=…, group=…, description=…, tags={…}]`), and .NET's `MetricName.ToString()`
     already produces that format. .NET can match Java exactly by reading all four fields.
  2. **D16 overrides only `ToString()`.** Java also uses this text wherever the exception is a
     cause (`KafkaException(Throwable)`, D13's `ExecutionException(Throwable)`). .NET's
     `CauseMessage` builds `FullName + ": " + Message`, and Java passes no message, so `Message`
     stays .NET's default ("Exception of type … was thrown."). The wrapping messages would
     therefore not be Java's text. Python covers both paths with `_java_to_string`, which
     `_throwable.to_string` prefers.
  3. **Two translation details.** `metric.metricName()` must become .NET's `IMetric.Name`, and the
     doubles need Java's `Double.toString` (finding 5).
- **Evidence:**
  - `rust/src/ffi/common/errors/quota_violation_error.rs:118` (returns
    `*const kafka_common_MetricName_t`), and the `kafka_common_MetricName_description` and
    `kafka_common_MetricName_tags` exports.
  - `rust/src/common/metrics/quota_violation_error.rs:43-48` (the core's struct).
  - `python/_confluentkafka.c:3889-3905` (Python reads `name` and `group` only).
  - `org.apache.kafka.common.metrics.QuotaViolationException#toString()`.
  - `python/confluent_kafka/common/metrics/quota_violation_error.py` (`_java_to_string`,
    `__str__`).
- **Suggested fix:**
  - D7: "C exposes the full `MetricName` (name, group, description, tags), and .NET rebuilds it in
    full so the text matches Java's; Python reads only the name and group."
  - D16: "The override's text is also what `CauseMessage` produces for this class: an internal
    virtual Java-`toString()` that `KafkaException` implements as `FullName + ": " + Message` and
    that the generated class overrides."

Resolved: D7 states C exposes the full `MetricName` and .NET reads all four fields; D16 and D21 make the class override the internal Java-`toString()` that every wrapping message uses; `Double.toString` comes from `JavaText` (D21).

## 12. [Low] The renderer doesn't cover the abstract `partitions()` getter and its overrides

- **Plan section:** Phase 2 Renderer ("getter properties for the fields of the payload classes").
- **Problem:** The consumer `partitions()` getters don't fit a field-only renderer:
  - `org.apache.kafka.clients.consumer.InvalidOffsetException#partitions()` is abstract and has no
    field behind it.
  - `OffsetOutOfRangeException#partitions()` returns `offsetOutOfRangePartitions.keySet()`.
  - `NoOffsetForPartitionException#partitions()` returns its field and has no `@Override`.

  A renderer that handles only fields either drops `Partitions` from the public API of these
  classes and of `LogTruncationException`, which inherits it, or emits a non-`override` property
  under an abstract base, which doesn't compile (CS0534). The parser already tells the three cases
  apart (`GetterBody::Abstract`, `KeySet`, `Field`).
- **Evidence:** the Java members above; `rust/xtask/src/error_hierarchy.rs:2875-2882`, where the
  Python renderer handles `KeySet` and `Abstract`.
- **Suggested fix:** Phase 2: "a getter property for every Java getter: `abstract` where Java's
  getter is abstract, `override` where a base class declares the same getter (decided from the
  hierarchy, not from `@Override`), and a key view for `keySet()`." Add an abstract-getter case to
  the renderer's unit tests.

Resolved: D3 "Implementation (revised)" makes every Java getter a property (`abstract`, `override` from the hierarchy, a key view for `keySet()`); Phase 2's renderer tests include an abstract getter and its override.

## 13. [Low] D10 should also make `TopicMetadataAndConfig` take an `ApiException`, as Java and Python do

- **Plan section:** D10.
- **Problem:** Java's public constructor is `TopicMetadataAndConfig(ApiException exception)`, and
  its field is an `ApiException`. Python takes `exception: ApiError | None`. .NET's constructor
  takes a `KafkaException`, because .NET had no `ApiException`. Once the hierarchy exists, keeping
  `KafkaException` is a deviation the plan doesn't record. It is the same kind of mismatch D9
  fixes for `ListGroupsResult.Errors()`.
- **Evidence:**
  `org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig#TopicMetadataAndConfig(ApiException)`;
  `python/confluent_kafka/admin/create_topics_result.py` (`exception: ApiError | None`);
  `dotnet/src/Confluent.Kafka/Admin/CreateTopicsResult.cs:188,218`.
- **Suggested fix:** Add to D10, marked ⚠: "The constructor and the field take `ApiException`, as
  Java's do. The marshal that builds them from a core per-topic error casts the class
  `FromFfiError` returns." List the change in the release notes, with the other ⚠ items.

Resolved: D9 retypes `TopicMetadataAndConfig`'s constructor and field to `ApiException` (⚠), with the marshal cast and the non-`ApiException` rule; D10 refers to it.

# Round 2 (verification of the revised plan)

## 5. [Low, was Medium] Generated messages need Java's `String.valueOf`, which C# concatenation doesn't give

- **Plan section:** D3 (constructors that build their message), D16, Phase 2.
- **Problem:** Several Java constructors, and the one factory, build their message with `+` on
  values that are not `String`s, and rely on `String.valueOf`. A literal C# translation gives
  different text:
  - a collection prints its CLR type name;
  - `null` prints as empty;
  - `bool` prints as `True`;
  - `double` uses the current culture and drops Java's `.0`.

  Python's generator wraps every such operand in `java_str` (`python/confluent_kafka/_java.py`,
  with `_double_to_string` for `Double.toString`). .NET has no equivalent; the only Java-text
  helpers are the private `JavaName` enum switches. Affected:
  - `TopicAuthorizationException(Set<String>)`: "Not authorized to access topics: " + the set, which
    Java prints as `[a, b]`.
  - `InvalidTopicException(Set<String>)`: "Invalid topics: " + the set.
  - `NoOffsetForPartitionException(Collection<TopicPartition>)`: "… for partitions: " + the
    collection.
  - Consumer `OffsetOutOfRangeException(Map<TopicPartition, Long>)`: Java prints the map as
    `{t-0=5}`.
  - `LogTruncationException(Map, Map)`: Java prints `{t-0=OffsetAndMetadata{…}}`.
  - `ConfigException(String, Object[, String])`: "Invalid value " + value, which Java prints as
    `null`, `true` or `[a, b]`.
  - `org.apache.kafka.common.errors.GroupAuthorizationException#forGroupId(String)`: a null id
    prints as `null`.
  - `org.apache.kafka.common.metrics.QuotaViolationException#toString()`: the value and bound
    print through Java's `Double.toString`, for example `9.0` or `1.0E23`.
- **Evidence:** the Java members listed. `Graph::translate`'s `Expr::Add` arm wraps every operand
  that is not a string literal in `java_str(...)` (`rust/xtask/src/error_hierarchy.rs:1589-1603`).
  Error messages are part of the behavioural contract (definition-of-done §3).
- **Suggested fix:** Add to Phase 2: "Generated code formats each non-`String` operand of a Java
  `+` with an internal `JavaString.ValueOf(object?)`, the counterpart of Python's `java_str`:
  - `null` → `"null"`;
  - `bool` → `"true"` / `"false"`;
  - `double` / `float` → Java's `Double.toString` / `Float.toString`, in the invariant culture;
  - a collection → `[e1, e2]`;
  - a map → `{k=v}`;
  - anything else → its `ToString()`.

  The renderer's unit tests and the class tests assert Java's text for each constructor above."

Resolved: D21 defines `JavaText.ValueOf` with these rules, and Phase 2's files use it for every non-`String` `+` operand; the renderer and `JavaText` tests cover a set, a map, a double, `null` and a boolean (Phases 1 and 2).

**Reopened (verification round, now Low): a `String` operand is still not formatted Java's way.**

The collection, map, boolean, double and non-`String` `null` cases are fixed (D21's rules). What
remains:
- **`String` operands.** Phase 2's Files bullet applies `JavaText.ValueOf` to "every non-`String`
  operand of a Java `+`". That wording comes from my own suggested fix, and it is wrong. Java's `+`
  prints a null `String` as `null`, and C#'s `+` prints it as empty, so two of the members this
  finding lists still get the wrong text:
  - `org.apache.kafka.common.errors.GroupAuthorizationException#forGroupId(String)`
    (`"Not authorized to access group: " + groupId`);
  - `org.apache.kafka.common.config.ConfigException#ConfigException(String,Object,String)`
    (`… " for configuration " + name`).

  Python wraps every operand except a string literal and a `String`-typed conditional
  (`rust/xtask/src/error_hierarchy.rs:1589-1603`: `Expr::Str` and an `Expr::Ternary` typed
  `String` are kept as they are; everything else becomes `java_str(...)`, a `String`-typed name
  included).
- **Elements.** D21's "a collection → `[e1, e2]`, a map → `{k=v}`" doesn't say how the elements
  are formatted. Java's `AbstractCollection#toString` and `AbstractMap#toString` append each
  element, key and value through `String.valueOf`. So a null element prints `null`, and a `Long`
  value prints without regard to the locale, where .NET's `long.ToString()` uses the current
  culture.

Fix:
- Phase 2: format every operand of a Java `+` except a string literal and a `String` conditional
  through `ValueOf`; a non-null `string` passes through unchanged.
- D21: `ValueOf` formats collection elements and map keys and values through itself, and numbers in
  the invariant culture, as `TopicPartition.ToString` already formats its partition
  (`dotnet/src/Confluent.Kafka/Common/TopicPartition.cs:64-65`).
- Add a null `groupId`, a null configuration name and a null set element to the renderer's
  message tests.

Resolved (round 2): Phase 2's Files bullet formats every operand of a Java `+` except a string literal and a `String`-typed conditional through `JavaText.ValueOf`, as Python's `Expr::Add` arm wraps them, so a null `String` prints `null`; D21 says `ValueOf` passes a non-null `string` through, formats integers in the invariant culture, and formats collection elements and map keys and values through itself; Phase 2's class tests cover a null `groupId`, a null configuration name and a null set element, and Phase 1's `JavaText` tests a null element and a `long` under a non-invariant culture.

## 14. [Medium] D14 cannot be done in C# in Phase 5: the generated `Code` overrides fix its accessibility

- **Plan section:** D4 "Implementation (revised)", D14, Phase 1, Phase 2 "Files", Phase 5, §5's mode
  split.
- **Problem:**
  - Phase 1 makes `Code` `public virtual`, so in Phase 2 the generator emits a `public override int
    Code` in every class with an id. Phase 5 then makes `Code` `internal` (D14).
  - C# does not let an override change the accessibility of the member it overrides. Every
    generated file then fails with CS0507.
  - Only the Rust generator can change those files: they are marked `GENERATED, DO NOT EDIT`, and
    `check-generated` diffs them. Phase 5 is Mode A (`dotnet-actor`, which does not author Rust), so
    it cannot complete D14 as planned.
- **Evidence:**
  - The C# specification's rule for override methods and properties: an override "cannot change the
    accessibility" of the overridden member (CS0507).
  - The plan text:
    - Phase 1: "Make `Code` `virtual` (D4)".
    - Phase 2 "Files": "the `Code` override (D4)".
    - Phase 5: "D14. Make `Code` internal".
    - §5: "Phases 1, 3, 4, 5 and 6 are Mode A".
- **Suggested fix:** Make the generated code independent of D14. For example, in D4:
  "`KafkaException` declares an internal `virtual int` id member (for example `FfiCode`, −1). Each
  generated class with an id, and `RecordDeserializationException`, overrides that member. `Code`
  is a non-virtual property that returns it, so D14 changes only `Code`'s modifier."

  Alternatively, since D14 is a §3 decision that Phase 0 settles, make `Code` `internal virtual` in
  Phase 1 when D14 is accepted, together with D14's `InternalsVisibleTo` entries, so the generator
  emits `internal override` from the start.

  Either way, the problem exists only if D14 is accepted. If `Code` stays public, nothing changes.

Resolved: folded into D14 as an explicit part of the choice ("How `Code` and the generated overrides fit"): (i), recommended, has the generated classes override an `internal virtual int FfiId` with `Code` a non-virtual property returning it, so the generated files do not depend on D14 and Phase 5's change is one modifier; (ii) keeps `Code` virtual and sets its final accessibility (with the `InternalsVisibleTo` entries) in Phase 1. D4's implementation, Phase 1, Phase 2's Files and Phase 5 follow (i) and name (ii) as the alternative. Which of the two is the user's call within D14; both are now implementable.

## 16. [Low] D13's "with no message" is wrong for a send's future

- **Plan section:** D13, Phase 6's timeout tests.
- **Problem:**
  - D13 makes every timed-out `Get` throw the `java.util.concurrent` `TimeoutException` "with no
    message, as Java's `get(long, TimeUnit)` throws it". A send's future is one of the types D13
    covers.
  - That is true of `KafkaFutureImpl`, whose `CompletableFuture` throws `new TimeoutException()`.
  - It is not true of Java's send future, which gives the exception a message.
- **Evidence:**
  - `org.apache.kafka.clients.producer.internals.FutureRecordMetadata#get(long,TimeUnit)`:
    `throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.")`.
  - Python's `_timed_out()` returns the exception with no message for every future, a send's
    included (`python/confluent_kafka/common/kafka_future.py:360-365`). That gap is on Python's
    side.
  - .NET's send future is the D13 type: `IProducer.Send` returns `KafkaFuture<RecordMetadata>`
    (`dotnet/src/Confluent.Kafka/Producer/IProducer.cs:68,91`).
- **Suggested fix:** In D13: "A send's future (`Send`'s `KafkaFuture<RecordMetadata>` and
  `AsyncKafkaFuture<RecordMetadata>`) times out with Java's `FutureRecordMetadata` message,
  `Timeout after waiting for <ms> ms.`; every other future times out with no message." Phase 6's
  timeout tests then assert both texts.

Resolved: D13 now gives a send's future Java's `FutureRecordMetadata` message, "Timeout after waiting for <ms> ms.", and every other future none, noting that Python's gap is on Python's side; Phase 6's timeout tests assert both texts.

## 17. [Low] Phase 3 tests D18's API branch through a C factory that does not exist

- **Plan section:** Phase 3 "New `FromFfiErrorClassTests`", D18.
- **Problem:**
  - The test list builds "real core errors … through the C factories" and expects "an
    unknown-server API error gives `UnknownServerException`".
  - BH has no factory that builds an API error with id −1. The `kafka_common_Error_kafka*_err`
    factories build a bare `KafkaException`, and no factory takes an error code.
  - So D18's `is_api_error`-true branch cannot be tested that way.
- **Evidence:**
  - BH has 34 `kafka_common_Error_*_err` factories, from `kafka`, `kafka_message`, `kafka_source`
    and `kafka_message_source` through `record_batch_too_large`. None builds `UnknownServer`, and
    no `*_from_code` function builds an error.
  - Python tests this branch with a fake library
    (`test_from_ffi_error_minus_one_is_unknown_server_error`,
    `python/test/unit/test_errors.py:840-842`).
- **A related gap.** Phase 3 says "For −1 it applies D18" of the last arm. The wrap of a callback's
  exception is another arm, which "builds the class of the core's id". The core builds that wrap
  as a bare `KafkaError`, so it has id −1 and `is_api_error` false. It is built by
  `ConsumerUtils::maybe_wrap_as_kafka_error` (`rust/src/consumer/internals/consumer_utils.rs:223-237`)
  and by `Error::kafka_source` (`rust/src/common/error.rs:2842-2844`). Both are Java's
  `new KafkaException(t)`, as in
  `org.apache.kafka.clients.consumer.internals.ConsumerUtils#maybeWrapAsKafkaException(Throwable)`.
  If that arm looks −1 up in the registry, it gets `UnknownServerException`, and no test in the
  plan's list would notice.
- **Suggested fix:** In Phase 3:
  - "The bare branch is tested with `kafka_common_Error_kafka_err`."
  - "The API branch is tested through an internal seam that gives `FromFfiError` id −1 with
    `ErrorIsApiError` 1, as Python's test does and as Phase 4 tests its readers."
  - "The callback-wrap arm resolves the id as the last arm does, D18 included; a test asserts that
    the wrap of a carried non-Kafka exception is exactly `KafkaException`."

Resolved (with 108#20; the related gap with 116#21): Phase 3 tests D18's bare branch with `kafka_common_Error_kafka_err` and its API branch through an internal seam that gives the classification an id and an `is_api_error` value, as Python fakes the library; the callback-wrap branch resolves −1 through D18, with its own test.

## 18. [Low] `RecordDeserializationException`'s registry entry calls a constructor the class does not have

- **Plan section:** D7 ("Its registry entry builds it from the message"), D8, Phase 2 "Registry".
- **Problem:**
  - The registry generated in Phase 2 builds each class from a message and a cause.
  - The hand-written `RecordDeserializationException` has one public constructor, which takes ten
    arguments. D3 adds the core-path constructor only to generated classes.
  - So the registry's −27 entry does not compile, and the plan does not say what the record fields
    hold for an error built that way.
- **Evidence:** `dotnet/src/Confluent.Kafka/Common/Errors/RecordDeserializationException.cs:32-43`,
  the only constructor. It takes origin, partition, offset, timestamp, timestamp type, key, value,
  headers, message and cause.
- **Suggested fix:** In D8, either:
  - "In Phase 2 the hand-written `RecordDeserializationException` gains the internal core-path
    constructor `(CoreError, string?, Exception?)`, chained to `SerializationException`'s, with
    these field values: …" (state them); or
  - give −27 no registry entry, and record that a core −27, which .NET never receives (D7), falls
    back to `KafkaException`.

Resolved: D8 gives the hand-written `RecordDeserializationException` no core-path constructor and −27 no registry entry; a core −27, which .NET never receives (D7), falls back to a bare `KafkaException`, recorded in §7. D7's registry bullet and Phase 4's table say no reader is needed.

## 19. [Low] D3 keeps Java's message-parameter names `s` and `msg` on public constructors

- **Plan section:** D3 (the table), §7 (".NET naming forms").
- **Problem:**
  - D3 keeps Java's parameter names and renames only the `Throwable` (to `innerException`).
  - Some Java exceptions name the message `s` or `msg`. Their generated public constructors would
    take `string? s` or `string? msg`.
  - So would the D15 classes, because `JDK_BUILTINS` lists `("String", "s")`.
  - C# named arguments make parameter names part of the source API, and every other exception in
    the binding uses `message`.
  - Python has an explicit rule for this: the message parameter is always `message`.
- **Evidence:**
  - `org.apache.kafka.common.InvalidRecordException#InvalidRecordException(String)` (`String s`).
  - `org.apache.kafka.common.errors.OutOfOrderSequenceException#OutOfOrderSequenceException(String)`
    and `org.apache.kafka.common.errors.ProducerFencedException#ProducerFencedException(String)`
    (`String msg`).
  - `rust/xtask/src/error_hierarchy.rs:631-636` (`JDK_FOUR`'s `("String", "s")`).
  - The root `CLAUDE.md`, Python Binding Conventions, Errors: "the `String` passed unchanged to
    `super(...)` as the message … is the keyword `message`, whatever Java calls it".
- **Suggested fix:**
  - In D3: "The `String` a constructor passes unchanged to `super(...)` as its message, directly or
    through `this(...)`, is named `message` whatever Java calls it, as in Python's Errors rule.
    Other parameters keep Java's names."
  - In §7's naming bullet, add "`message` for Java's `s` / `msg`".

Resolved: D3's new "Parameter names" bullet names the message parameter `message` whatever Java calls it (`s`, `msg`, the `("String", "s")` rows of `JDK_BUILTINS`), as Python's Errors rule; §7's naming bullet records it; Phase 2's renderer tests include a message parameter Java names `s` or `msg`.

## 20. [Low] D21 and D22 change existing public messages but carry no ⚠

- **Plan section:** D21, D22, §3's ⚠ rule, §6 "Behaviour changes users can see", Phase 1's Exit.
- **Problem:**
  - Under D22 (a), the `Message` of an exception built without a message changes from .NET's
    default text ("Exception of type '…' was thrown.") to `""`. That covers `new KafkaException()`,
    `new KafkaException((string?)null)` and `new SerializationException()`.
  - D21's `JavaText.ToString` replaces `CauseMessage`. So `new KafkaException(cause)`, for a cause
    whose Java message is null, changes from "<type>: Exception of type '…' was thrown." to
    "<type>".
  - Both change existing public behaviour, but neither carries the ⚠ that §3 gives such
    decisions. §6's list (D4, D5, D6, D9, D13, D14, D15, D18, D20, D23) omits both, and Phase 1's
    Exit names only D22.
- **Evidence:**
  - `dotnet/src/Confluent.Kafka/Common/KafkaException.cs:44-72` (the public constructors) and
    `:94-95` (`CauseMessage`).
  - `dotnet/src/Confluent.Kafka/Common/Errors/SerializationException.cs`.
  - The plan's §3 intro: "⚠ marks the ones that change existing public behaviour".
- **Suggested fix:** Mark D21 and D22 ⚠, add both to §6's list, and name D21 in the Exit of the
  phase that replaces `CauseMessage`. Phase 1 adds the helper but doesn't say whether it also
  switches `KafkaException(Exception cause)` and `WrapAtPoll` to it.

Resolved (with 116#19 and 108#22): D21 and D22 are marked ⚠ and listed in §6; D21 says Phase 1 switches `KafkaException(Exception cause)`, `WrapAtPoll` and the local-callback wrap to `JavaText.ToString` and deletes `CauseMessage`; Phase 1's Exit names D4, D21's cause message and D22.
