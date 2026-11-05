# COMMENTS.107: Critic 107 on `dotnet/design/history/M22/P1-error-hierarchy-parity/PLAN.md` — resolved

Moved from `COMMENTS.107.md` after the plan revision; each finding ends with how and where it was resolved.

## 3. Medium · D13: a timed-out `Get` should throw D15's Java-named `TimeoutException`, not `System.TimeoutException`

**Problem.** D13 says "Unchanged, as in Java: a timeout still throws `System.TimeoutException`".
That is wrong about Java. `KafkaFuture.get(long, TimeUnit)` throws
`java.util.concurrent.TimeoutException`, which is the class D15 adds at the root. Python raises
its Java-named root class there, not `builtins.TimeoutError`, and with no message, as
`CompletableFuture` does. D15's class derives from `System.TimeoutException`, so following
Python here breaks no existing `catch`.

**Evidence.**
- `python/confluent_kafka/common/kafka_future.py:362-367` (`_timed_out` builds
  `confluent_kafka.timeout_error.TimeoutError` with no message), raised at `:730`, `:901`, `:904`.
- .NET throws `new TimeoutException()` (`System.TimeoutException`) at
  `Internal/KafkaFutureImpl.cs:155`, `:307`, `:391`. Its `Message` is .NET's default text, not
  Java's `null`.

**Fix.** In D13, replace the timeout clause with: "a timeout throws
`Confluent.Kafka.TimeoutException` (D15; a `System.TimeoutException`), as Java's
`java.util.concurrent.TimeoutException` and Python's `confluent_kafka.TimeoutError`." Add it to
Phase 6's `KafkaFutureImpl` change and to the translated `KafkaFutureTest` cases.

Resolved: D13's timeout bullet now throws the D15 `TimeoutException` with no message, as Java and Python; Phase 6 applies it in `KafkaFutureImpl` and the translated `KafkaFutureTest` cases.

Verified (Critic 107, verification round): the revised plan text fixes it.

## 4. Medium · D13: "the error's `toString()` form" is not defined for .NET

**Problem.** In .NET, `Exception.ToString()` gives the type, the message, the inner exceptions
and the stack trace. Java's `Throwable.toString()`, and Python's translation of it, gives
`"<class>"`, plus `": <message>"` only when the message is non-null, and uses the class's own
override where one exists (`QuotaViolationException`, D16). The plan relies on this form for
`ExecutionException`'s message but never defines it. The existing helper, `CauseMessage`, also
falls short: it always appends `": " + Message` and ignores an override. The same helper also
builds `WrapAtPoll`'s message and the local-callback wrap's message.

**Evidence.**
- `python/confluent_kafka/_throwable.py:86-98` (`to_string`) and `:110-113` (`cause_message`).
- `ExecutionError` uses it: `python/confluent_kafka/execution_error.py`, the `cause_message`
  branch of its constructor.
- .NET: `Common/KafkaException.cs:94-95` (`CauseMessage`), used at `:98` and `:170`.

**Fix.** Add to D13 (and D16): "one internal `JavaToString(Exception)` helper: the D16 override
when the class has one; otherwise the type's full name, plus `": " + Message` only when Java's
message is non-null. It is used for `ExecutionException(cause)`, every generated
`(Throwable cause)` constructor, `WrapAtPoll` and the local-callback wrap." Record in §7 that the
name is the .NET type's full name, as Python records that it uses the Python class path.

Resolved: D21 defines `JavaText.ToString(Exception)` (override first, then type name plus `": " + message` only for a non-null Java message), replacing `CauseMessage`, used for `ExecutionException`, every `(Throwable cause)` constructor, `WrapAtPoll` and the local-callback wrap; the .NET type name is D21's recommendation, recorded in §7.

Verified (Critic 107, verification round): the revised plan text fixes it.

## 6. Low · D7, D16: `Message` of a core-built `QuotaViolationException` and `NoOffsetForPartitionException`

**Problem.**
- **`QuotaViolationException`.** Java's constructor passes no message, so `getMessage()` is
  `null`. Python deliberately makes `str(e)` the `toString()` override's text after the class
  name. D16 only says `ToString()` is overridden, and D7's core path builds the class with the
  core's message. So .NET's `Message` would be neither Java's nor Python's.
- **`NoOffsetForPartitionException`.** Python drops the core's message and builds it from Java's
  constructor, from the partitions. The plan keeps the core's message.

**Evidence.**
- `python/confluent_kafka/_errors.py:162-164` and `:174-175`
- `python/confluent_kafka/common/metrics/quota_violation_error.py:79-84` (`__str__`)
- Root `CLAUDE.md`, Errors: "A class that overrides `toString()` (`QuotaViolationException`)
  uses its override: `str(e)` is the override's text after the class name".

**Fix.** Add to D7: "the readers build `QuotaViolationException` and
`NoOffsetForPartitionException` through their Java constructors, as Python's `_CORE_KWARGS` does,
not through the message-only core constructor." Add to D16: "`Message` of
`QuotaViolationException` is the override's text after the class name (Python's deviation from
Java's `null`), or Java's `null`, recorded in §7." Pick one.

Resolved: D7 now builds `QuotaViolationException` and `NoOffsetForPartitionException` with Java's message, as Python's `_CORE_KWARGS`; D16 recommends Python's choice for `QuotaViolationException.Message`, recorded in §7.

Verified (Critic 107, verification round): the revised plan text fixes it.

## 7. Medium · D4 against Phase 3: what code an unknown id reports

**Problem.** D4 says a bare `KafkaException` "as a fallback, reports −1". Phase 3 says the
fallback is "a bare `KafkaException(code, …)` for an unknown id", which keeps the unknown code.
Python's fallback is the class `KafkaError`, whose `_ffi_id` is −1. Its gRPC server and its soak
client read that class id, so an unknown id crosses as −1.

**Evidence.**
- `python/confluent_kafka/_errors.py:82-94` (`class_for_ffi_id`, `to_ffi_id`)
- `python/grpc_translate.py:113-121` and `:190-191`
- `python/soak/soakclient.py:365-379` (`error_code` reads `type(ex)._ffi_id`)

**Fix.** Make Phase 3 match D4: "an unknown id falls back to a bare `KafkaException` with `Code`
−1, as Python's `KafkaError`." Or, if keeping the core's code is wanted for diagnostics, record
it in §7 and say what the gRPC server sends. Add the case to the "unknown id falls back" test.

Resolved: Phase 3's fallback is now a bare `KafkaException` reporting −1 (D4), tested in `FromFfiErrorClassTests`; the stored code is deleted in Phase 3, so no bare `KafkaException` keeps another code.

Verified (Critic 107, verification round): the revised plan text fixes it.

## 8. Medium · D11: the premise about Python is wrong; Python raises no Kafka class for these gaps

**Problem.** D11 defers the 15 "the admin result produced no …" errors until Phase 3 "checks what
Python raises where its marshalling finds the same gap". The answer is already in the code.

Where a single value is missing, Python raises nothing. It substitutes `None`, an empty list or
`""`:
- `delegation_token_to_py` returns `None` for a NULL token;
- `conv_feature_metadata` and `conv_transaction_description` return `None`;
- `conv_partition_producer_state` returns `[]`;
- a NULL transactional id becomes `""`.

Where a whole result or future is missing, Python raises the built-in `RuntimeError`, not a
`KafkaError`. Its gRPC server then sends −4 with "python server: RuntimeError: …".

.NET throws a bare `KafkaException`, which the plan keeps at −1. That differs from Python in
type, in gRPC code, and in whether anything is raised at all.

**Evidence.**
- Python substitutes values: `python/_confluentkafka.c:8031-8033`, `:8128-8142` (the `tid ? tid : ""`
  line), `:8550-8554`, `:8571-8579`, `:8582-8586`.
- Python raises `RuntimeError`: `python/_confluentkafka.c:8978`, `:9008-9011`.
- Python's gRPC fallback: `python/grpc_translate.py:193-200`.
- .NET: `Internal/Interop/DelegationTokenMarshal.cs:110`, `:174`; `TransactionMarshal.cs:55`,
  `:74`; `PartitionProducerStateMarshal.cs:32`, `:66`; `TransactionDescriptionMarshal.cs:32`,
  `:67`; `FeatureMetadataMarshal.cs:33`, and the rest of the 15.

**Fix.** Replace D11's third bullet with the facts above and a ruling. Either follow Python per
site (return `null` or empty where Python does; throw `InvalidOperationException`, which the
server sends as −4, where Python raises `RuntimeError`), or keep the bare `KafkaException` and
list it in §7. Java has no counterpart either way, so this is not a Java-fidelity question.

Resolved: D11's last bullet now states Python's actual behaviour (substitutes `None`/`[]`/`""` per value, raises `RuntimeError` for a missing result or future, sent as −4) and offers (a) follow Python per site (recommended) or (b) keep the bare `KafkaException`, recorded in §7.

Verified (Critic 107, verification round): the revised plan text fixes it.

## 9. Medium · D12, D14: Python's soak client keeps its code tables, read through the private id

**Problem.** D14 says that with `Code` internal, "the soak client's code checks … become class
checks". D12 says that in that case both clients change together, "as a separate change to the
Python client". Python's soak client has no public code to lose. It reads the private class
attribute `_ffi_id` and keeps its code tables: auth, topic-exists, coordinator and disconnect.
So the parity-preserving form of D14 is that .NET's soak reads the internal `Code` through
`InternalsVisibleTo`, exactly as D14 already lets the gRPC server do, and keeps its tables. No
Python change is needed.

D14 also lists only three checks (`TopicAlreadyExistsCode`, `UnknownTopicOrPartitionCode`, the
D12 auth list). .NET's soak has two more code tables, `s_coordinatorErrorCodes` and
`s_disconnectErrorCodes`.

Finally, .NET's `ErrorCode` reads only a `KafkaException`, while Python's `error_code` reads any
class's id. After D6 and D15 that matters: the root `TimeoutException` (−5) and
`CorrelationIdMismatchException` (−24) are not `KafkaException`s.

**Evidence.**
- Python: `python/soak/soakclient.py:209-226`, `:365-379`, `:1443-1450`, `:1671-1680`.
- .NET: `dotnet/soak/SoakClient/SoakClient.cs:58`, `:65`, `:78`, `:88-89`, `:306`, `:1093`,
  `:1104`, `:1200-1205`, `:1307-1314`.

**Fix.** Rewrite D14's soak bullet: "the soak client reads the internal `Code` through
`InternalsVisibleTo`, as Python's reads `_ffi_id`, and keeps its tables; `ErrorCode` reads the id
of every class that has one, the D15 root classes included." Drop D12's "separate change to the
Python client".

Resolved: D12 and D14 rewritten: the soak reads the internal `Code` through `InternalsVisibleTo` and keeps all its tables, coordinator and disconnect included; `ErrorCode` reads any class's id through the generated type → id lookup (Phase 2), the D15 classes included; no Python change.

Verified (Critic 107, verification round): the revised plan text fixes it. One follow-up gap in the id lookup it relies on (it covers only "the D15 classes", not `CorrelationIdMismatchException`) is new comment 13 in `COMMENTS.107.md`.

## 11. Low · D15, §2: the JDK class set does not match `JDK_BUILTINS` or Python

**Problem.** D15 says the JDK classes are "generated from the generator's `JDK_BUILTINS` table",
and its table lists six. `JDK_BUILTINS` has eight: it also has
`java.util.NoSuchElementException` and `java.lang.IndexOutOfBoundsException`. Python ships both
as root classes. D15's sentence "Python gives each Java built-in exception the binding raises its
own Java-named root class" then lists only six. §2's count ("The 7 JDK classes (`Exception`,
`RuntimeException` and the 5 built-ins)") agrees with neither D15's six nor the table's eight.

**Evidence.**
- `rust/xtask/src/error_hierarchy.rs:637-702`
- `python/confluent_kafka/no_such_element_error.py`, `index_out_of_bounds_error.py`
- `ERROR-CATEGORIZATION.md` §5.1 ("the 8 JDK built-ins at the root") and the §5.2 table
- Python raises them from `common/record/timestamp_type.py:51` and `admin/_mock_core.py:243-261`.
  .NET has no `TimestampType.ForName`, and its mocks are core-backed.

**Fix.** Say that the C# renderer emits the six and skips the other two because nothing in .NET
raises them, and record that in §7. Or emit all eight. Correct §2's count to match.

Resolved: D15 now names all eight Python classes, explains why two are skipped (no C id, nothing in .NET throws them), offers generating all eight, and §7 records the skip; §2's count is corrected to 6 of 8.

Verified (Critic 107, verification round): the revised plan text fixes it.

## 12. Low · §7 is incomplete

**Problem.** §7 says that with every recommendation accepted, "only these remain". These
differences from Java or Python are left out, beyond those raised in comments 2, 4, 5, 8, 10
and 11:

- **Bases.** §7 names only `ConcurrentModificationException`'s base. D15 equally gives
  `IllegalStateException`, `IllegalArgumentException` and `UnsupportedOperationException` a .NET
  base in place of Java's `RuntimeException`. `KafkaException : Exception` stands in for Java's
  `extends RuntimeException`, where Python's `KafkaError(RuntimeError)` keeps that chain.
- **`RecordDeserializationException`.** D8 keeps its .NET-specific shape (`byte[]` buffers,
  `Headers`, the nested enum). Python generates it from Java's constructors.
- **Sync `ToCompletionStage()`.** Python's sync `KafkaFuture` has `to_completion_stage()`, and so
  does Java's `KafkaFuture`. .NET's sync futures have none (`dotnet/CLAUDE.md` §4, Futures). D13
  claims parity only for the async form.
- **Audience.** Non-`@InterfaceAudience.Public` error classes become public (for example
  `CorrelationIdMismatchException` in `common.requests`). Python's Errors rule states this
  carve-out explicitly ("a class for every exception the FFI enum names, Public or not"); the plan
  generates them without saying so.

**Evidence.**
- Python's sync `to_completion_stage`: `python/confluent_kafka/common/kafka_future.py:652-659`.
- `python/confluent_kafka/common/kafka_error.py:32`.
- Root `CLAUDE.md`, Python Binding Conventions, Scope and Errors.

**Fix.** Add these items to §7, each with its reason. Or drop "only these remain" and say that §7
lists the deviations left inside the error hierarchy.

Resolved: §7 now lists the bases (D15 and `KafkaException`), `RecordDeserializationException`'s shape (D8), the sync futures' missing `ToCompletionStage()` (today's recorded deviation), and says the non-Public classes are public as Python's Errors rule does; its scope is stated as the error hierarchy and future failure path.

Verified (Critic 107, verification round): the revised plan text fixes it.

# Round 2 (verification of the revised plan)

## 13. Low · Phase 2 registry, D12, Phase 3 tests: the type → id lookup leaves out `CorrelationIdMismatchException`

**Problem.** Phase 2 limits the registry's type → id lookup to "the classes that are not
`KafkaException`s (the D15 classes)". `CorrelationIdMismatchException` (−24) is not a
`KafkaException` either under D6(a): it derives from the Java-named `IllegalStateException`. So it
has no `Code` (D4 puts `Code` on `KafkaException`) and no lookup entry. Three things then miss it:
- D12's `ErrorCode`, which "reads the id of every class that has one … through the generated id
  lookup";
- Phase 5's gRPC rule ("reads `Code` for a `KafkaException` and the registry's type → id lookup
  for the rest"). Phase 3 sends −24 through a hand-written arm instead;
- Phase 3's `ErrorHierarchyTests`, "each class's `Code` matches the C enum", which has no `Code` to
  read for it.

Python's class carries its id like every other class, and its soak client and gRPC server read it
the same way as any other.

**Evidence.**
- `python/confluent_kafka/common/requests/correlation_id_mismatch_error.py:41`
  (`_ffi_id = -24`).
- `python/soak/soakclient.py:365-379`.
- `python/grpc_translate.py:190-191`.

**Fix.** In Phase 2's registry bullet, make the lookup cover "every generated class that is not a
`KafkaException`: the D15 classes and `CorrelationIdMismatchException`". Let Phase 3's −24 arm use
the lookup. Make `ErrorHierarchyTests` check those classes' ids through the lookup.

Resolved (with 108#23): Phase 2's type → id lookup covers every class with a C id, including those that do not derive from `KafkaException` (the D15 classes and `CorrelationIdMismatchException`); D12's `ErrorCode`, Phase 5's gRPC server (one lookup arm replacing Phase 3's type arms) and soak, and Phase 3's `ErrorHierarchyTests` read ids through it.

## 14. Low · D23 (a): "Java's message" cannot be reproduced; state the .NET message

**Problem.** Java's message is `"Can't convert key of class " + record.key().getClass().getName()
+ " to class " + producerConfig.getClass(KEY_SERIALIZER_CLASS_CONFIG).getName() + " specified in
key.serializer"`. It names the serializer class from the `key.serializer` config entry. .NET has no
such config entry: serializers are constructor instances (`dotnet/CLAUDE.md` §4, Config;
`ProducerConfig` takes no class names). So the plan cannot keep "Java's message" verbatim. It
should state the text it uses: the key's runtime type, the serializer instance's type, and the
`key.serializer` / `value.serializer` wording.

There is also nothing in .NET that matches Java's typical trigger. That is a
`ClassCastException` at the erased generic boundary when the record's key type does not match the
serializer's `T`. .NET's generics are reified, so that cast cannot fail, and only a user
serializer's own `InvalidCastException` would be wrapped. That is still Java-faithful, since Java's
`catch` wraps that too, but it is worth one sentence in D23.

**Evidence.**
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:1080-1092`.
- Python wraps nothing: `python/confluent_kafka/producer/_base.py:251-262`.

**Fix.** In D23 (a), replace "with Java's message" with the concrete .NET message. Add a sentence
that the wrap catches only an `InvalidCastException` thrown inside the serializer, because .NET's
reified generics cannot produce Java's erased-cast failure.

Resolved: D23 now states the .NET message ("Can't convert key of class <the key's runtime type> to class <the serializer's runtime type> specified as the key serializer", the same for the value, `null` for a null key) and says the wrap catches only an `InvalidCastException` the serializer throws itself, because .NET's reified generics cannot produce Java's erased-cast failure.
