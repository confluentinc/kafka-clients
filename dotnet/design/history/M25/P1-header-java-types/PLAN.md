# Plan: Java's header types in the .NET binding (M25/P1)

Status: **approved 2026-11-21** (Pranav, every recommendation of §9: D1 = A1, D2–D8 and D11 as recommended).
D9 is done: `8b66f1f25` adds the interface rule to CKRDA0013 as the finding kind `kind`, with
`kind T:Confluent.Kafka.Common.Header.Header` and `kind T:Confluent.Kafka.Common.Header.Headers` in
`D/analyzers/java-name-baseline.txt`; this phase deletes both lines. D10: on `prashah_dev_dotnet_additional_rules`
at `8b66f1f25`, in the main checkout, no push. N = 131; ledger rows from 596. S1 (Rust tooling) goes to the
actor-executor, reviewed by the kafka-critic; the C# steps to the dotnet-actor, reviewed by the dotnet-critic.
Mode A: .NET only, plus one commit in the shared Java-API generator (`rust/xtask`). No change to the Rust core,
the C ABI, the C header, Python or the gRPC proto.

Path prefixes: `J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `N/` = `dotnet/src/Confluent.Kafka/`,
`NT/` = `dotnet/tests/Confluent.Kafka.UnitTests/`, `D/` = `dotnet/`, `R/` = `rust/`. Lines read at `e6308cc68`.

## 1. TL;DR

- **Problem.** Java has two header interfaces (`Header`, `Headers`) and two classes that implement them
  (`RecordHeader`, `RecordHeaders`). .NET has only two classes, named after the interfaces.
- **Recommendation: option A1.** Add the interfaces `IHeader` / `IHeaders`, rename the classes to Java's
  `RecordHeader` / `RecordHeaders` in Java's package (`Confluent.Kafka.Common.Header.Internals`), and make every
  public signature use the interfaces, as Java's do. The Rust core and the C ABI already use these Java names.
- **For a user.** `new Header(k, v)` becomes `new RecordHeader(k, v)`, one more `using` line, and code that
  names the types writes `IHeader` / `IHeaders`. A user can now write their own header class, as in Java.

## 2. The problem

Java (`J/common/header/`): `Header` (`Header.java:25-26`) and `Headers` (`Headers.java:27-28`) are
`@InterfaceAudience.Public` interfaces. The only classes that implement them are `RecordHeader`
(`internals/RecordHeader.java:26`) and `RecordHeaders` (`internals/RecordHeaders.java:29`): public classes in the
package `common.header.internals`, **not** marked `@InterfaceAudience.Public`. Every public Java signature names
only the interfaces (`ProducerRecord.java:71`, `:111`, `:158`; `ConsumerRecord.java:118`, `:188`;
`Serializer.java:84`; `Deserializer.java:86`; `RecordDeserializationException.java:76`, `:118`). Users still
build headers with `new RecordHeader(...)`, because Java has no other class to build one with. Kafka itself does
too (streams 4 files, tools 1, server 1, and the consumer's own `DefaultRecord.java:500`).

```java
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

var record = new ProducerRecord<>("orders", null, "k", "v",
        List.of(new RecordHeader("trace-id", "abc".getBytes(UTF_8))));
for (Header h : consumerRecord.headers()) { use(h.key(), h.value()); }
```

.NET today (`N/Common/Header/`): one sealed class per Java interface. `Header` (`Header.cs:31`) carries
`RecordHeader`'s constructor, equality and string form (it prints `RecordHeader(key = …)`). `Headers`
(`Headers.cs:28`) carries `RecordHeaders`' three constructors. Signatures take and return these classes.

```csharp
using Confluent.Kafka.Common.Header;

var record = new ProducerRecord<string, string>("orders", null, "k", "v",
    new[] { new Header("trace-id", Encoding.UTF8.GetBytes("abc")) });
foreach (Header h in consumerRecord.Headers()) { Use(h.Key(), h.Value()); }
```

What this shape costs today:

1. A Java interface became a class. Our naming rule 1 says a Java interface is an `I`-prefixed C# interface
   (`D/CLAUDE.md:884-885`). The analyzer does not check the kind yet, so nothing flags it.
2. A user cannot write their own header type. Java allows it; `Header` and `Headers` are `sealed`.
3. Java's `headers(String)` is renamed `GetHeaders`, because a C# class cannot have a method named like itself
   (CS0542). It is allowlisted (`D/analyzers/java-name-allowlist.txt:8-10`).
4. Inside any `Confluent.Kafka.Common.*` namespace, the name `Header` means the namespace, not the class (CS0118),
   so library code writes `Header.Header` (`D/CLAUDE.md:181-186`).
5. Java's public `RecordHeaders.setReadOnly()` / `isReadOnly()` (`RecordHeaders.java:112`, `:116`) are
   `internal` here (`Headers.cs:98`, `:287`). Java's `RecordHeader(ByteBuffer, ByteBuffer)`
   (`RecordHeader.java:38`) is missing.
6. The four constructors are allowlisted as "H1" (`java-name-allowlist.txt:27-32`, ledger 269).

What the others do:

- **Python** (root `CLAUDE.md:503-504`, Behaviour with no other home): the same one-class form as .NET today,
  `Header(key=…, value=…)` and `Headers(headers=…)`, recorded as a deviation: "Java's concrete class is the
  internal `RecordHeader`, so a user needs a public class to build one". A user cannot pass their own header
  (`python/confluent_kafka/common/header/headers.py:59-64` checks `isinstance(item, Header)`). The read-only
  state is private (`headers.py:199`, `:204`).
- **Rust core**: Java's names. `Header` / `Headers` are traits (`R/src/common/header/mod.rs:35-59`), and
  `RecordHeader` / `RecordHeaders` are public structs, kept public on purpose with
  `#[doc(alias = "public-in-rust")]` (`internals/record_header.rs:29`, `internals/record_headers.rs:34`). The
  `internals` module is crate-private and re-exports them at `common::header` (`mod.rs:21`, `:32`). Its public
  signatures use the concrete structs, not the traits (`mod.rs:71`; `producer_record.rs:444`;
  `consumer_record.rs:500`), so a Rust user cannot plug in their own header.
- **C ABI**: Java's names, `internals` included: `kafka_common_header_internals_RecordHeader_t` and
  `_RecordHeaders_t`, with `kafka_common_header_Header_t` / `_Headers_t` views (`__as_Header`, `__as_Headers`).
  No C type lets a user implement a header.

## 3. The options

C# terms used below, one line each:
- **interface**: a contract with no data; any class can implement it. Rust: a trait. Python: a `Protocol`.
- **sealed**: no class can derive from it. Rust structs are always like this.
- **explicit interface implementation**: a member callable only through the interface, used when its name
  clashes with the class (CS0542).
- **covariance**: a `RecordHeader[]` can be passed where `IEnumerable<IHeader>` is expected, with no copy.

### A1 (recommended): Java's names in Java's packages

Public API, before and after:

```csharp
// before                                          // after
namespace Confluent.Kafka.Common.Header;           namespace Confluent.Kafka.Common.Header;
public sealed class Header {                       public interface IHeader { string Key(); byte[]? Value(); }
    public Header(string key, byte[]? value);      public interface IHeaders : IEnumerable<IHeader> {
    public string Key(); public byte[]? Value();       IHeaders Add(IHeader header);
}                                                      IHeaders Add(string key, byte[]? value);
public sealed class Headers : IEnumerable<Header> {    IHeaders Remove(string key);
    public Headers(); (Header[]?); (IEnumerable<Header>?);  IHeader? LastHeader(string key);
    public Headers Add(Header header); …               IEnumerable<IHeader> Headers(string key);   // Java's name
    public IEnumerable<Header> GetHeaders(string key);  IHeader[] ToArray();
    public Header[] ToArray();                     }
}                                                  namespace Confluent.Kafka.Common.Header.Internals;
                                                   public sealed class RecordHeader : IHeader {
                                                       public RecordHeader(string key, byte[]? value);
                                                       public RecordHeader(ReadOnlyMemory<byte> keyBuffer,
                                                                           ReadOnlyMemory<byte>? valueBuffer); // D4
                                                   }
                                                   public sealed class RecordHeaders : IHeaders {
                                                       public RecordHeaders(); (IHeader[]?); (IEnumerable<IHeader>?);
                                                       public void SetReadOnly(); public bool IsReadOnly();  // D5
                                                   }
// signatures, before → after
ProducerRecord(…, IEnumerable<Header>? headers)  →  ProducerRecord(…, IEnumerable<IHeader>? headers)
Headers ProducerRecord.Headers()                 →  IHeaders ProducerRecord.Headers()       // ConsumerRecord too
ISerializerWithHeaders<T>.Serialize(string topic, Headers headers, T? data)  →  (…, IHeaders headers, …)
```

User code, before and after:

```csharp
// before
using Confluent.Kafka.Common.Header;
var headers = new[] { new Header("trace-id", traceBytes) };
foreach (Header h in record.Headers()) { … }
// after
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
var headers = new[] { new RecordHeader("trace-id", traceBytes) };
foreach (IHeader h in record.Headers()) { … }
```

Pros:
- Name for name with Java: interfaces, classes, packages, signatures, `Headers(key)`, `SetReadOnly()`,
  `IsReadOnly()` and the `ByteBuffer` constructor. Java code translates line by line.
- Same names as the Rust core and the C ABI (the C ABI even keeps `internals`).
- A user can implement `IHeader` / `IHeaders`, as in Java.
- Removes the CS0118 `Header.Header` clash, the `GetHeaders` rename and seven allowlist lines.
- Fits our own layout rule as it is: "every public type lives in the namespace of its Java package"
  (`D/CLAUDE.md:104-106`, ledger 253) and `PublicNamespaceLayoutTests` (`NT/PublicNamespaceLayoutTests.cs:588-593`)
  need no special case. The analyzer needs no code change.

Cons:
- Needs a recorded exception to root `CLAUDE.md` §2 (an `internals` package, not `@InterfaceAudience.Public`).
  It is the same exception the Rust core takes for the same two classes.
- A public namespace named `Internals` reads oddly in .NET, and sits beside our non-public `Internal/` folder.
- Every line of user code that names a header type changes. Allowed: the client is in preview.
- Python keeps a different shape (`Header(...)` classes).

### A2: Java's names, classes beside the interfaces (the Rust core's path)

As A1, but `RecordHeader` / `RecordHeaders` live in `Confluent.Kafka.Common.Header`, as the Rust core re-exports
them at `common::header`. User code needs one `using` line, not two.

Pros: as A1, without a public `Internals` namespace.
Cons: breaks our layout rule (namespace = Java package, ledger 253), so it needs a second exception, two
`java-name-pairs.txt` lines and special rows in `PublicNamespaceLayoutTests`. A Java `import …internals.…` line
no longer maps to a `using` line.

### B: interfaces, our class names

`IHeader` / `IHeaders` as in A1. The classes keep the names `Header` / `Headers` in `Common.Header`, recorded as
translating `RecordHeader` / `RecordHeaders`.

```csharp
public sealed class Header : IHeader { public Header(string key, byte[]? value); … }
public sealed class Headers : IHeaders {
    IEnumerable<IHeader> IHeaders.Headers(string key) { … }   // explicit: CS0542 forbids a public Headers(…) here
    …
}
// user code: new Header(k, v) still compiles; foreach (Header h in record.Headers()) compiles with a hidden cast
```

Pros: signatures and the interfaces match Java; `new Header(k, v)` keeps compiling; no `internals` name is public;
the class names match Python.
Cons:
- Class names differ from Java, the Rust core and the C ABI. A Java user looks for `RecordHeader` and does not
  find it.
- Keeps the CS0118 clash, and `Headers(key)` is only reachable through the interface.
- **Not cheaper in tooling than A1.** The analyzer pairs a type by its exact name before it reads
  `java-name-pairs.txt` (`D/analyzers/Confluent.Kafka.Analyzers/JavaNameAnalyzer.cs:224-234`), so the class
  `Header` pairs with Java's interface `Header`, not `RecordHeader`. A pair also needs its Java type in
  `java-api.txt` (`JavaNameAnalyzer.cs:359`). So B needs the same generator change as A1 **plus** an analyzer
  change, or it keeps the H1 and M4 allowlist lines and gains two for the planned interface rule.

### C: keep today's shape

No change. Python's one-class form, allowlisted.

Pros: no work, no break, same as Python.
Cons: every gap in §2 stays; users cannot implement a header; the planned "a Java interface is an `I<Name>`
interface" rule must allowlist `Header` and `Headers` for good.

### Rejected variant

The Rust core's full shape (Java names, but concrete classes in signatures) loses Java's interface signatures,
which is the point of the change. Not proposed.

## 4. Comparison

| | A1 | A2 | B | C |
|---|---|---|---|---|
| Java parity | full: names, packages, signatures, members | names and signatures; package differs | interfaces and signatures; class names differ | lowest: classes for interfaces |
| Python (`Header`/`Headers` classes) | differs | differs | class names match | same |
| Rust core / C ABI (`RecordHeader(s)`) | same names (C ABI: same package too) | same names and Rust's path | class names differ | differs |
| Rules exception | root §2, as the Rust core's `public-in-rust` | root §2 and ledger 253 | none for root §2; a recorded name pair | none (allowlist stays) |
| User code breaks | yes: `RecordHeader`, `IHeader(s)`, `Headers(key)`, serde implementers | yes, one `using` fewer | partly: signatures and serde implementers | no |
| Users can implement a header | yes | yes | yes | no |
| Effort | medium: 1 xtask + 4 .NET commits, mostly mechanical | medium plus layout special cases | medium plus an analyzer change | none |

## 5. Recommendation

**A1.** Java parity is the goal, and A1 is the only option where Java code maps to .NET name for name. The
exception it needs is not new: the Rust core already keeps exactly these two classes public
(`public-in-rust`), and the C ABI already exports them under Java's `internals` name. So .NET joins the core and
the C ABI instead of keeping a deviation of its own. It also removes three .NET-only oddities (CS0118, `GetHeaders`,
a class standing for an interface) and costs nothing on the hot path (§6, S2). The price is a one-time break,
allowed in preview, and a shape that differs from Python's.

It also fits the planned rule "a Java interface is a C# `I<Name>` interface": if that rule lands first, its
`Header` / `Headers` findings go in the baseline and this phase deletes them; the two `KafkaFuture` delegates stay
allowlisted as planned. Once signatures use interfaces, the binding can later hand out another `IHeaders` for
fetched records (for example Java's lazy form, one copied buffer per record decoded on first read, as
`DefaultRecord.java:500` does with `RecordHeader(ByteBuffer, ByteBuffer)`) with no further API break. A
native-backed view stays the deferred, risky choice of `D/CLAUDE.md` §6.4.

## 6. Implementation steps

One commit each. Every commit builds on every TFM and passes the unit tests, `dotnet format --verify-no-changes`
and the analyzers (CKRDA0013's lists only shrink). The Critic reviews after each one.

**S1 — the generator lists `RecordHeader` and `RecordHeaders`.** (`R/xtask`, shared tooling.)
- `R/xtask/src/java_api.rs`: a new list beside `TEST_JAR_CLASSES` (`:46`) for main-jar classes the bindings keep
  public, holding the two classes, its comment naming the Rust core's `public-in-rust` tag; `is_listed`
  (`:273-294`) reads it. `TEST_JAR_CLASSES` cannot just take them: it applies only to test-jar classes
  (`class.is_test`, `:280-284`). The `HEADER` text (`:48-64`) and module docs say so.
- Regenerate `D/analyzers/java-api.txt` (only the .NET copy exists: `BINDING_COPIES`, `:42`; Python reads none).
- Verify: `cargo test` in `rust/xtask`, `cargo xtask java-api --check ../dotnet/analyzers/java-api.txt`,
  `cargo xtask check-generated`, `cargo xtask format-check`, `cargo xtask lint`, the pre-commit hook (a Rust
  change). Then a .NET build: CKRDA0013's findings are unchanged, since no .NET type pairs with the new Java types
  yet (`cargo xtask dotnet-java-name-baseline` writes nothing).

**S2 — the interfaces, and every signature uses them.** (Names `Header` / `Headers` unchanged in this commit.)
- New `N/Common/Header/IHeader.cs`, `IHeaders.cs`. `Header : IHeader`; `Headers : IHeaders`, with
  `IHeaders.Headers(string)` explicit for now (CS0542).
- Signatures: `ProducerRecord` (`ProducerRecord.cs:77`, `:145`, `:218`), `ConsumerRecord` (`ConsumerRecord.cs:115`,
  `:163`, `:221`), `RecordDeserializationException` (`:49`, `:105`), `ISerializerWithHeaders` (`ISerializer.cs:86`),
  `IDeserializerWithHeaders` (`IDeserializer.cs:75`) and their extension methods (`ISerializer.cs:124`,
  `IDeserializer.cs:116`), `SerdeGuard`'s reflection types (`Internal/SerdeGuard.cs:32`, `:45`).
- Internals: the record's header list holds `IHeader`. `ProducerRecord` keeps its own concrete headers object,
  as Java's `new RecordHeaders(headers)` (`ProducerRecord.java:85`), so `KafkaProducer.cs:152` and
  `AsyncKafkaProducer.cs:186` still seal it with no cast. The send reads each header's `Key()` once and
  `Value()` once through the interface (`Internal/Interop/ProducerSendMarshal.cs:106-118`, `:246-268`). A user
  header with a `null` key throws Java's `IllegalArgumentException("Invalid null header key found in headers")`
  (`J/common/record/internal/DefaultRecord.java:563-565`, rethrown by `KafkaProducer.java:1151`) on every
  producer (D8). The consumer path is unchanged: it builds the binding's own objects
  (`ConsumerRecordsMarshal.cs:304-349`).
- Cost: one interface call per header beside the P/Invoke per header the send already makes
  (`Headers_add_with_key_value`); no allocation, so `NT/PublicProducerHeadersAllocationTests.cs` stays as it is.
  The header copy stays the recorded deviation from root §14 (ledger 301, `ffi-marshalling.md` §A4).
- **C ABI: no change.** The binding builds the native `RecordHeaders_t` itself from the managed key and value
  (`RecordHeaders_new_err`, `__as_Headers_err`, `Headers_add_with_key_value`; `NativeMethods.cs:713-735`), so it
  never hands the core a managed header object.
- Tests: the serde test doubles take `IHeaders`; the new tests of §7 for user headers.
- If CKRDA0013 cannot hold this half-way state without new list lines, S2 and S3 become one commit.

**S3 — rename and move to Java's package.**
- `Header` → `RecordHeader`, `Headers` → `RecordHeaders`, in `N/Common/Header/Internals/`, namespace
  `Confluent.Kafka.Common.Header.Internals`. `GetHeaders` → `Headers(string)`. `SetReadOnly()` and `IsReadOnly()`
  public methods (D5; today an internal property). The `ReadOnlyMemory<byte>` constructor (D4). Both stay `sealed` (D6).
- Delete the H1 and M4 allowlist lines (`java-name-allowlist.txt:8-10`, `:27-32`). Expect no new line in any list.
- `NT/PublicNamespaceLayoutTests.cs:315`, `:317`: four rows (`IHeader`, `IHeaders`, `RecordHeader`,
  `RecordHeaders`).
- Callers: 18 files outside `src/` (17 test files and `D/grpc-server/Translate.cs`; 136 constructor calls). Soak
  and tools name no header type.
- Verify: as above, plus `cargo xtask dotnet-java-name-baseline` (no change) and
  `make -C dotnet test-grpc-server-dotnet`.

**S4 — Java's tests on Java's names.** Rename `NT/PublicHeadersTests.cs` to `RecordHeadersTests.cs`, Java's
`RecordHeadersTest` in Java order on `RecordHeader` / `RecordHeaders`; the two thread-safety tests run on the
`ReadOnlyMemory<byte>` constructor, which is the form Java wrote them for (§7).

**S5 — docs and records.**
- `D/CLAUDE.md`: §2 layout tree (`Header/Internals/`), the CS0118 note (`:181-186`, keep the rule, drop the
  `Header` example), the `Internal/` marker sentence (`:98-103`: `Common/Header/Internals/` is Java's package
  and public); §3 sketches (`:239-243`, `:440`), the headers row of the type table (`:782`), the serde headers
  bullet (`:730-734`); a short rule recording the exception (D2).
- `ffi-marshalling.md` §A4 (`:713-724`): headers read through `IHeader`.
- Ledger rows (one per decision of §9), `design/current/STATUS.md`.

**S6 — close-out.** The full gate, `make verify-dotnet` (`D/CLAUDE.md` §7.5; on macOS the integration target
skips itself, so run the native arms), then archive `COMMENTS.DONE.<N>.md` here.

## 7. Tests

Translate (Java `J/../test/…/common/header/internals/RecordHeadersTest.java`; Kafka 4.4 has no `RecordHeaderTest`):
- All 14 tests, in Java order, on the Java names. `testHeadersIteratorRemove` and the two `iterator.remove()`
  checks of `testReadOnly` stay out: a .NET enumerator has no `remove()` (as today).
- `testRecordHeaderIsReadThreadSafe` and `testRecordHeaderWithNullValueIsReadThreadSafe`: `@RepeatedTest(100)`
  as 100-pass loops, 16 tasks each, on the `ReadOnlyMemory<byte>` constructor (D4).
- `RecordHeader`'s own checks follow the Java source: the null-key message, `Equals` / `GetHashCode` /
  `ToString`, the buffer constructor decoding on first read.

Add (binding behaviour):
- A user `IHeader` passed to `ProducerRecord`, to `RecordHeaders.Add`, or added by a serializer, is sent: read
  back from the native record (as `NT/Interop/ProducerRecordBuildTests.cs` does), on the sync and async producers.
- A user header with a `null` key: Java's `IllegalArgumentException` and message, on the four producers.
- `ProducerRecord.Headers()` is a `RecordHeaders` copy of the input; `RecordHeaders(IEnumerable<IHeader>)` copies a
  user `IHeaders`.
- `ConsumerRecord` and `RecordDeserializationException` take a user `IHeaders` and return the same object.
- A `RecordHeader` never equals a user `IHeader` with the same key and value (Java's `getClass()` check,
  `RecordHeader.java:68-77`).
- `SetReadOnly()` / `IsReadOnly()`: the producer seals; a user can seal their own.
- Shape: the layout rows; `PublicJavaMemberShapeTests` (no public property); the xtask unit test for the new list.

## 8. Risks

1. **A public `Internals` namespace** can confuse readers, next to our rule that `Internal/` is the only
   non-public folder. Mitigation: S5 says so in one sentence; the folder names differ (`Internals` is Java's).
2. **A badly written user header** (a `null` key, values that change between reads, a throw) now reaches the send.
   Mitigation: each value is read once; a `null` key gets Java's error; a throw leaves the send unchanged, as a
   throwing serializer does. Memory safety does not depend on the user: the binding only pins a `byte[]` it got
   back, for one call.
3. **Mock producers differ from Java on a `null` key.** Java's `MockProducer` never checks keys; ours build the
   record through the C ABI, which cannot carry one. Same precedent as the NUL in a header key
   (`ProducerSendMarshal.cs:103-105`). Recorded (D8).
4. **Shared tooling.** S1 edits `rust/xtask`. Mitigation: it only adds two types to the list; `--check` and
   `check-generated` guard the copy.
5. **Open branches that touch headers or serdes** (for example the paused M23/P7 serdes-from-config work) will
   need a rebase. Check at approval.
6. **Python and .NET diverge.** A user of both sees `Header(...)` in one and `RecordHeader(...)` in the other.
   Python's conventions are not changed here (D11).

## 9. Open decisions for Pranav

1. **Option.** A1, A2, B or C? *Recommended: A1.*
2. **Where the exception is recorded.** A short rule in `D/CLAUDE.md` §3 plus a ledger row: "`RecordHeader` and
   `RecordHeaders` are public, as in the Rust core (`public-in-rust`); no other class from an `internals` package
   or outside `@InterfaceAudience.Public` is". Root `CLAUDE.md` stays as it is unless you edit it yourself
   (agents do not change it). *Recommended: yes, as written.*
3. **Generator list.** A new list for main-jar classes the bindings keep public, beside `TEST_JAR_CLASSES`.
   No automatic cross-check against the Rust tags for now. *Recommended: yes.*
4. **`RecordHeader(ByteBuffer, ByteBuffer)`.** Translate as `RecordHeader(ReadOnlyMemory<byte> keyBuffer,
   ReadOnlyMemory<byte>? valueBuffer)`, decoded on first read as Java's, or allowlist it?
   *Recommended: translate.* It is the form Java's consumer uses, and Java's two thread-safety tests target it.
5. **`SetReadOnly()` / `IsReadOnly()` public**, as Java's (Python keeps them private)? *Recommended: public.*
6. **`sealed`.** Java's classes are open. *Recommended: keep `sealed`*, as every .NET value class here
   (`ProducerRecord`, …); users implement the interfaces instead.
7. **No compatibility shim** (no `[Obsolete] Header` class). *Recommended: none.* The client is in preview, and a
   shim would keep the CS0118 clash and two names for one thing.
8. **A user header with a `null` key** throws Java's `IllegalArgumentException` from `Send` on all four
   producers, the mocks included (risk 3). *Recommended: yes.*
9. **Order with the `I<Name>` interface rule.** The rule lands first with `Header` / `Headers` in its baseline,
   and this phase deletes those lines. *Recommended: yes* (the lists only shrink; either order works).
10. **Base branch.** On top of `prashah_dev_dotnet_additional_rules` if the interface rule lands there, else
    `master`. *Recommended: decide at approval; default on top of the rule's branch.*
11. **Python.** No change in this phase; the divergence is recorded in the ledger. *Recommended: yes.*

### Corrections to the brief (checked in the code)

- Python does not read `java-api.txt`: the generator writes only the .NET copy (`java_api.rs:42`).
- `TEST_JAR_CLASSES` only covers test-jar classes (`java_api.rs:280-284`), so S1 needs a new list.
- Outside `src/`, 18 files call `new Header(` / `new Headers(` (136 calls); the count of 22 included four
  generated files under `grpc-server/obj/`.
- In Kafka's clients main code, two files call `new RecordHeader(`: `DefaultRecord.java:500` (the `ByteBuffer`
  form, on the consumer path) and `RecordHeaders.java:73`.
- The Rust core's public signatures use the concrete `RecordHeader(s)`, not the traits (§2).
