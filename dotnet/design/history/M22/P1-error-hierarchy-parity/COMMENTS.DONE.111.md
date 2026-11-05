# Critic 111: review of Phase 2, the generated error classes (working tree)

Scope: the uncommitted Phase 2 changes on top of `4daee5f4d`, reviewed against
`dotnet/design/history/M22/P1-error-hierarchy-parity/PLAN.md` (Phase 2, D3, D4, D6, D7, D8, D15, D16, D18,
D19, D21, D22, D24 and §7), the Java sources in `kafka/` and the header's `kafka_common_Error_e`
(`rust/target/include/confluent_kafka_bindings.h`). Files reviewed:
- the generated `Internal/ErrorClasses.g.cs`;
- every generated class under `Common/Errors`, `Common/Config`, `Common/Metrics`, `Consumer`,
  `Producer`, `Java/Lang`, `Java/Util` and `Java/Util/Concurrent`;
- the hand-written `Internal/Interop/ErrorPayloadMarshal.cs`, `Internal/KeySetView.cs`,
  `Common/Metrics/KafkaMetric.cs` and `RecordDeserializationException.cs`;
- the tests `GeneratedErrorClassesTests.cs`, `ErrorClassRows.g.cs`, `PublicNamespaceLayoutTests.cs` and
  the edited unit and gRPC tests.

The generator itself is kafka-critic 111's scope; it is cited below only where the fix goes there.
Two findings, both Low.

## 1. Low: `KafkaException(Exception? cause)` is the one Java `Throwable` parameter not named `innerException`, and the rename leaves `SerializationException`'s API changed, contrary to D8

- **File:** `dotnet/src/Confluent.Kafka/Common/KafkaException.cs:83`:
  `public KafkaException(Exception? cause)`.
  `dotnet/src/Confluent.Kafka/Common/Errors/SerializationException.cs` (generated): at `4daee5f4d` its
  line 52 read `public SerializationException(Exception? cause)`; it is now
  `public SerializationException(Exception? innerException)`.
- **Plan:**
  - D3's table maps `Throwable` to `Exception? innerException`.
  - §7 records ".NET naming forms only: `innerException` for Java's `cause`".
  - D8 says `SerializationException` "becomes generated; its four public constructors match Java's, so
    its API is unchanged apart from `Code` (D4)".
- **What is wrong:**
  - After this phase every generated `(Throwable)` constructor names its parameter `innerException`. So
    does every `(String, Throwable)` constructor, `KafkaException`'s included. The hand-written base
    `KafkaException(Exception? cause)` is the only exception among the classes this plan owns. A caller
    who writes `new ApiException(innerException: e)` cannot write
    `new KafkaException(innerException: e)`: that is CS1739, no parameter named `innerException`.
  - `RecordDeserializationException`'s ten-field constructor
    (`RecordDeserializationException.cs:42`) also names it `cause`. D8 leaves that class's shape to
    decision 22, outside this plan, so this finding does not ask for that rename.
  - The rename is also a source break that D8 says does not happen. `new SerializationException(cause: e)`
    compiled at `4daee5f4d` and now fails with CS1739. No call site in `dotnet/` uses `cause:`, so nothing
    in the tree breaks, but the plan's "API unchanged" claim no longer holds.
- **Fix:**
  - Rename `KafkaException`'s parameter to `innerException`. The class is hand-written (D8); the body
    becomes `: this(innerException is null ? null : JavaText.ToString(innerException), innerException)`.
    Every `Throwable` parameter in the classes this plan owns then has one name, as D3 and §7 say.
  - Ask the Manager to amend D8's wording to "unchanged apart from `Code` and the cause parameter's
    name, now `innerException` (D3)". Alternatively, record the named-argument break where the plan
    lists the public API changes.

**Resolution (Actor 111):** `KafkaException(Exception? innerException)` at
`dotnet/src/Confluent.Kafka/Common/KafkaException.cs:83`, body
`: this(innerException is null ? null : JavaText.ToString(innerException), innerException)`. No
`cause:` named argument on a KafkaException-family constructor exists in `dotnet/`; the one hit,
`RecordDeserializationExceptionTests.cs:78`, is RecordDeserializationException's own `cause`, left
as decision 22 has it. The plan's D8 wording was amended by the Manager.


## 2. Low: `AuthenticationException`'s XML summary ends "raise one of the subclasses of this exception:." because Java's list is dropped

- **File:** `dotnet/src/Confluent.Kafka/Common/Errors/AuthenticationException.cs:24-28` (generated):
  `/// requested and raise one of the subclasses of this exception:. Java's`
- **Java:** the Javadoc of `org.apache.kafka.common.errors.AuthenticationException` continues after the
  colon with a `<ul>` of the four subclasses it means:
  - `SaslAuthenticationException`;
  - `UnsupportedSaslMechanismException`;
  - `IllegalSaslStateException`;
  - `SslAuthenticationException`.

  Python's generated `AuthenticationError` docstring keeps all four.
- **What is wrong:** the C# renderer's `class_summary` (`rust/xtask/src/error_hierarchy/csharp.rs:1022`)
  keeps only the first paragraph of the Javadoc. That paragraph ends at the `<ul>` with a colon, and the
  renderer then appends `.` because the text does not end with one. The published doc therefore
  promises a list it does not give and ends in `":."`. IntelliSense and the API docs show this text for
  a public class. It is the only error Javadoc in the generated set with a list, so no other class is
  affected.
- **Fix:** in `class_summary`, render the Javadoc's `<ul>/<li>` as an XML-doc `<list type="bullet">` of
  `<item><description>…</description></item>` after the summary sentence, so the four subclasses are
  kept as Java and Python keep them. Alternatively, when the first paragraph ends with `:`, keep the
  following list paragraph as well. Then regenerate and let `check-generated` pin the output. Dropping
  only the colon would still lose the list, so it is the weaker fix.

**Resolution (Actor 111):** `class_summary` (`rust/xtask/src/error_hierarchy/csharp.rs:1031`) now
renders the `<ul>` that follows the first paragraph as `<list type="bullet">` with one
`<item><description>…</description></item>` per `<li>`, and adds no "." after the colon. The items
come from the new `JavaClass::javadoc_list` (`rust/xtask/src/java_parse.rs:147`, filled by
`javadoc_list_items`, `:547`); the plain text Python reads is unchanged. Regenerated:
`AuthenticationException.cs:24-38` lists the four subclasses.
