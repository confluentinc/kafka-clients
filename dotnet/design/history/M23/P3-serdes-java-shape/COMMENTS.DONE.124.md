# COMMENTS.DONE.124 — dotnet-actor 124, M23/P3 (the .NET serdes in Java's shape)

### 124-S1-1 (Low) The rule-7 `Dispose` exemption in CKRDA0013 also stops pinning Java's `close()` on every disposable client

`dotnet/analyzers/Confluent.Kafka.Analyzers/JavaNameAnalyzer.cs:411-420`

**What is wrong.** The new filter drops `close()` (no parameters) from `required` for any type whose Java
counterpart is `Closeable` / `AutoCloseable` and whose .NET type implements `IDisposable` / `IAsyncDisposable`. That
was added so `ISerde<T> : IDisposable` stands for Java's `Serde#close()` (and `WrapperSerde<T>.Dispose` for
`Serdes$WrapperSerde#close()`). But the filter is not limited to the serdes. It also applies to `IProducer`,
`IAsyncProducer`, `IConsumer`, `IAsyncConsumer`, `IAdmin`, `IAsyncAdmin` and their `Kafka*` / `Mock*` / `Async*`
classes. All of them are `IDisposable`, and their Java types declare `close()` (`Producer#close()`,
`Consumer#close()`, `Admin#close()` (a `default`), `KafkaProducer#close()`, `MockConsumer#close()`, …,
`analyzers/java-api.txt`). So the analyzer no longer requires a `Close()` on any of them. On these types the binding
keeps `Close()` next to `Dispose` with a different contract. `Close()` throws where `Dispose` must not: a consumer
`Close` from another thread during an op, or from inside a callback, throws `ConcurrentModificationException`, while
`Dispose` there does not throw or is deferred (`dotnet/CLAUDE.md` §3 sketches, §4 **Disposal**, **Consumer
deviation**). On the clients, `Dispose` does not stand for Java's `close()`.

**Evidence (measured, in a `git archive` copy under the scratchpad, nothing in the tree changed).** I added a probe test
to `JavaNameAnalyzerTests`. Its source has `public interface IProducer : IDisposable { void Flush(); }`, with Java
`Producer interface Closeable`, `close()`, `flush()`, and it expects
`missing T:Confluent.Kafka.Producer.IProducer org.apache.kafka.clients.producer.Producer#close()`.
- At `819f5e9b7`'s analyzer: **0 diagnostics** (the test fails: "expected 1 actual 0"). A `Close()` deleted from
  `IProducer` would now pass the build.
- With `6f2c27605`'s `JavaNameAnalyzer.cs` swapped in, the probe **passes** (the diagnostic comes back), and the two
  new analyzer tests fail, so they do guard the S1 fix.
The claim that no baseline line outside the serdes changed is true: nothing is hidden today. What is lost is the guard
for the clients from now on.

**What a fix must do.** Keep the serde result (no `Serde#close()` / `Serdes$WrapperSerde#close()` finding) without
unpinning `close()` on the clients. For example:
- drop the filter and add the two lines to the allowlist, under the group plan §3 already plans for
  `Serializer#close` / `Deserializer#close` ("Java's `close()` is `IDisposable`, rule 7"); or
- limit the filter to the serialization interfaces and classes.

Either way, add an analyzer test like the probe above: an `IDisposable` client type with no `Close()` still reports
Java's `close()` as missing. Its control is `ACloseablesClose_IsDisposeOnATypeThatIsIDisposable`.

**Resolved:** e789a5c2e (fixup! of 819f5e9b7) — the rule-7 filter is removed from JavaNameAnalyzer, so close() is required on every IDisposable type again; ISerde<T>'s Serde#close() and WrapperSerde<T>'s Serdes$WrapperSerde#close() are allowlisted with the reason; ACloseablesClose_IsStillRequiredOnATypeThatIsIDisposable pins an IDisposable IProducer without Close() (and an IDisposable ISerde) to the missing close() finding, and fails with S1's filter put back.

---


### 124-S4-1 (Medium) The mock producers hand the serializers the record's own headers; Java's and Python's mock hand them fresh, empty ones

`dotnet/src/Confluent.Kafka/Internal/SerializedProducerRecord.cs:61-75` (the `wrapInvalidCast: false` path that
`MockProducer.cs:119` and `AsyncMockProducer.cs:147` take), and the `headers` docs of
`ISerializerWithHeaders<T>.Serialize` (`Common/Serialization/ISerializer.cs`, "A producer passes the record's own headers,
which the record carries when it is sent").

**What is wrong.** On `MockProducer` / `AsyncMockProducer` a headers-form serializer gets `record.Headers()`. So it sees
the headers the record already has, and the headers it adds end up on the user's `ProducerRecord`. Java's mock does not
do that. Neither does Python's, nor the core mock the .NET mocks run on.

**Evidence.**
- Plan §10 and row 456 cite `MockProducer.java:614-615`. Those lines are inside `MockProducer#partition(record, cluster)`.
  `send` calls that method only when `cluster.partitionsForTopic(topic)` is non-empty (`MockProducer.java:311-318`).
- The .NET mocks have no cluster. They are built on `kafka_producer_MockProducer_with_auto_complete_err`
  (`NativeProducer.cs:164`), which is `Cluster::empty()` (`rust/src/producer/mock_producer.rs:745-747`). That matches
  Java's `MockProducer(autoComplete, partitioner, keySerializer, valueSerializer)`, which also uses `Cluster.empty()`.
- With an empty cluster, Java always takes the other branch: `keySerializer.serialize(record.topic(), new RecordHeaders(),
  record.key())`, and the same for the value. Each call gets a fresh, empty headers object, and it is thrown away.
- Python's mock does the same (`python/confluent_kafka/producer/_mock_core.py:437-446`, `headers=Headers()` for each
  call).
- The core mock does the same. Its comment says "the empty-cluster branch with fresh ones … the fresh ones dropped"
  (`mock_producer.rs:1327-1350`).

**Where the divergence is fixed in place.**
- Two tests assert the non-Java behaviour: `SerdeHeadersTests.MockProducer_CallsTheHeadersForm_WithTheRecordsHeaders_AndLeavesThemWritable`
  and its async twin. Through `AssertSerializedWithTheRecordsHeaders` they check `Same(record.Headers(), seen)` and that
  the record's headers are now `["h", "schema-id"]`.
- The commit body says "the mocks as Java's MockProducer".
- The comment at `SerializedProducerRecord.cs:59-60` says the same thing.

**Needs Pranav.** Row 456 as written says "all clients pass the record's own `Headers`", and its stated reason does not
hold for the mocks.

**What a fix must do.** One of:
- (a) As Java and Python do: on the two mocks, call the headers form with a new empty `Headers` for the key and another
  for the value, and leave the record's headers unchanged. The mocks are off the hot path. Then correct the two tests,
  the `ISerializerWithHeaders` docs and the comment.
- (b) Keep the current behaviour and record it as a deviation, with Pranav's ruling on row 456.

**Resolved:** d27cbb91e (fixup! of 4744a62e8) — option (a), decided by the Manager under row 456 (Java's shape; Python agrees): MockProducer and AsyncMockProducer give each headers-form serializer call a new, empty Headers and drop it, as Java's MockProducer on its empty cluster; the record keeps its headers, unchanged and writable. The real producers still pass the record's own. The ISerializerWithHeaders docs, the internal comment and the two mock tests (now ..._WithNewEmptyHeaders_AndLeavesTheRecordsUnchanged) say so; putting the record's headers back on the mock path fails 4 tests.

---

### 124-S4-2 (Low) No test sends or polls with a key serde that has the headers form, on any of the eight classes

`dotnet/tests/Confluent.Kafka.UnitTests/SerdeHeadersTests.cs`, `dotnet/tests/Confluent.Kafka.IntegrationTests/SerdeHeadersBrokerTests.cs`.

**What is wrong.** In every send and every poll through a headers form, that form belongs to the value serde. The key
serdes are:
- `PlainSerializer` and `PlainDeserializer` in `SerdeHeadersTests`;
- `Serdes.String()` in the broker tests.

`SerdeGuardTests` does pass a headers-form key, but it only constructs the client. So the key half of each client's
cached type test (`_keyWithHeaders`) and of the two shared helpers is not tested.

**Evidence.** I mutated a `git archive` tree of `4744a62e8`:
- `SerializedProducerRecord.Serialize` gets `null` in place of `keyWithHeaders`, in both key calls;
- `ConsumerRecordsMarshal`'s key call becomes `Deserialize(keyDeserializer, null, …)`.

I wiped `bin`/`obj` and rebuilt: 0 warnings, 0 errors. The full unit suite then passes on net10.0: 8284 passed, 24
skipped, 0 failed. So a client that stops calling the key's headers form passes every test, for example one that never
assigns `_keyWithHeaders`.

**What a fix must do.**
- On the four producers, give the key a headers-form serializer that adds its own header. Assert:
  - it saw the same `Headers` object as the value's call;
  - the value's call sees the key's header, since Java passes one object to both. On the mocks, this follows whatever
    124-S4-1 decides.
- On the two mock consumers and the two broker tests, give the key a headers-form deserializer. Assert it saw the
  record's `Headers` object.
- Check that the key mutation above then fails.

**Resolved:** d27cbb91e (fixup! of 4744a62e8) — a headers-form key on all eight classes: unit tests on the two real producers (key and value see the record's Headers; the value sees the key's header), the two mock producers (each call its own new empty Headers; the record unchanged) and the two mock consumers (the key deserializer sees the record's Headers); the two broker round trips now use headers forms for key and value (4/4 passed on net10.0 against Docker). Key routing switched off: SerializedProducerRecord fails 4 tests, ConsumerRecordsMarshal 2.

---

### 124-S5-1 (Low) UTF-8 decoding of a surrogate written in UTF-8 gives one U+FFFD per byte; Java gives one per sequence

`dotnet/src/Confluent.Kafka/Internal/JavaCharset.cs:138` (`Kind.Utf8 => Utf8Marshal.GetString(data)`, which is
`Encoding.UTF8.GetString`), and the docs that claim Java's reading: `JavaCharset.cs:24-30` ("decodes as
`new String(bytes, charset)` … malformed input decodes to U+FFFD") and `Common/Serialization/StringDeserializer.cs:28-29`
("read as Java reads it").

**What is wrong.** For the 3-byte form of a surrogate (`ED A0..BF xx`), and for its truncated form at the end of the
data, .NET writes one U+FFFD per byte. Java writes one U+FFFD for the whole sequence. This affects
`StringDeserializer` with the default UTF-8 (so `Serdes.String()` too) and with a configured UTF-8 name, and the text
in `UUIDDeserializer`'s "Invalid UUID string: …" cause. The other eight charsets match Java (see below). The UTF-8 path
predates this phase, but S5 adds the docs that promise Java's reading, and plan R2 / row 467 ask for "Java's byte rules"
for the Unicode charsets.

**Evidence.** JDK 21 (`eclipse-temurin:21-jdk` in Docker), `new String(bytes, UTF_8)`, against `StringDeserializer`
built from an archive of `d945783b1`:

| bytes | Java | .NET |
|---|---|---|
| `ED A0 80` | `FFFD` | `FFFD FFFD FFFD` |
| `ED A0 80 ED B0 80` (CESU-8 of U+10000) | `FFFD FFFD` | six `FFFD` |
| `41 ED A0` | `0041 FFFD` | `0041 FFFD FFFD` |
| `ED BF` | `FFFD` | `FFFD FFFD` |

The JDK source is the same on master: `java.lang.String.decodeUTF8_UTF16`, 3-byte branch. It decodes the three bytes,
and for `Character.isSurrogate(c)` writes one `REPL`. A truncated `ED A0` passes `isMalformed3_2`, so it writes one
`REPL` and stops. In a random run of 8,000 vectors, all 168 differences were this case, all on the UTF-8 decode.
Python's codec gives what .NET gives (`b"\xed\xa0\x80".decode("utf-8", "replace")` gives three U+FFFD), but here Java
decides: the Actor followed the JDK over Python for UTF-16 and UTF-32.

**What a fix must do.** One of these:
- (a) Port the JDK's UTF-8 replacement rules into `JavaCharset`'s UTF-8 decode, for the serdes only. ABI strings come
  from Rust and are valid UTF-8, so `Utf8Marshal.GetString` can stay as it is for them. Add the four rows above to
  `JavaCharsetTests`. The receive path's allocation-budget tests must stay green.
- (b) Keep the decode and record it as a deviation (Python's reading). Then the two docs must stop saying "as Java
  reads it" for UTF-8.

**Resolved:** 405e6bb8a (fixup! of d945783b1) — JavaCharset's UTF-8 decode ports the JDK's String.decodeUTF8_UTF16 rules for malformed input (option (a)), for the serdes only: StringDeserializer (so Serdes.String()) and the UUID deserializer's UTF-8 path; Utf8Marshal and every other marshalled string are unchanged. Valid input keeps the single .NET decode (a result with no U+FFFD is returned as it is; any other is decoded again by the JDK's rules). The item's four rows plus every branch of the JDK decoder are in JavaCharsetTests with JDK 21's output, the four rows through StringDeserializer (default, "UTF8", Serdes.String()) and the UUID cause message in SerdeConfigureTests, and a net8.0+ test pins the fast path's allocation. Differential against JDK 21: 937,420 vectors, 0 differences (the old decode: 10,053). Mutants: the old decode fails 7 tests, no fast path 1, a UTF-8 surrogate replaced per byte 4.

---

### 124-S6-1 (Low) STATUS "Open" names the wrong `dotnet/CLAUDE.md` texts as left over after R1–R6

`dotnet/design/current/STATUS.md:21` ("R1–R6 and the two other stale texts in `dotnet/CLAUDE.md` (`:398-399`, the old
cast text; `:685-686`, Void's "deserialize default" and "one shared, stateless instance")").

**What is wrong.** This sentence is the record of what Pranav must change in the rulebook. It is wrong both ways.
- `:685-686` are not left over. They are inside R1's range (plan §7 R1: "replaces lines 670-686 of the code block").
- Four passages that no draft replaces still contradict the shipped code, and the sentence does not list them:
  - `:436`, the consumer sketch: `Headers()` "(not routed through a deserializer this phase)". S4 passes the record's
    `Headers` to the deserializer's headers form on all four consumers. R5 changes only `Key()` and `Value()` (`:434-435`).
  - `:646-648`: "Still to come: headers for the deserializers (row 153; …)". S4 shipped it, and row 456 lifts row 153.
  - `:738`, the idiom-map row for Java methods: "`Serdes.String()`, … `Void()`, each returning one shared stateless
    instance". S1 returns a new serde per call (row 471). R4 adds a row; it does not replace this one.
  - `:760`, the "Strings" call-site bullet: an unpaired surrogate is encoded as `?` "by `StringSerde`". S1 deleted
    `StringSerde`. Its successor `StringSerializer` writes `?` only for UTF-8: under S5's UTF-16 and UTF-32 charsets
    it writes U+FFFD (STATUS plan correction 4).

So if Pranav applies R1–R6 and the two texts listed, the rulebook still says that deserializers do not get headers and
that `Serdes` returns shared instances.

**Evidence.** `grep -n "not routed through a deserializer\|Still to come\|each returning one shared\|StringSerde"
dotnet/CLAUDE.md` at `25f9f67f2` gives the four lines. `git diff 6f2c27605..25f9f67f2 -- dotnet/CLAUDE.md` is empty,
so the line numbers are the plan's too.

**What a fix must do.** Correct the Open sentence:
- say R1 covers `:685-686`, or drop them;
- add `:436`, `:646-648`, `:738` and `:760`, each with what it should now say;
- keep `:398-399`.

**Resolved:** 041f3a370 (fixup! of 25f9f67f2) — the STATUS "Open" sentence no longer lists `:685-686` apart (it notes R1 replaces `:670-686`), keeps `:398-399`, and adds `:436`, `:646-648`, `:738` and `:760`, each re-checked at `25f9f67f2` with a quote of its text and what it should now say. `dotnet/CLAUDE.md` is unchanged.

---

### 124-R-1 (Low) R2's Headers bullet and R3's guard sentence state for every client what only the real clients do

`dotnet/CLAUDE.md:712-715` (R2, **Headers.**), `:891-897` (R3, the guard sentence) and `:732` (R2, **Encodings.**), at
`b3c1f548d`.

**What is wrong.** The applied text is the drafts' word for word (checked mechanically against PLAN §7). But three of
its claims do not match the shipped code:

1. **R2, Headers:** "The client tests the type once, at construction, and passes the record's own `Headers`: writable
   during the call, sealed after it on the real producers."
   - Only `KafkaProducer`, `AsyncKafkaProducer` and the four consumers pass the record's own headers.
   - Since `d27cbb91e` (124-S4-1, the Manager's decision under row 456), `MockProducer` and `AsyncMockProducer` pass a
     new, empty `Headers` to each headers-form call and drop it, as Java's `MockProducer` does on its empty cluster.
     The code is `SerializeOnMock` → `withHeaders.Serialize(topic, new Headers(), data)`
     (`Internal/SerializedProducerRecord.cs:87-89`), and its comment at `:57-60` says so.
   - Two tests pin this: `SerdeHeadersTests.MockProducer_CallsTheHeadersForm_WithNewEmptyHeaders_AndLeavesTheRecordsUnchanged`
     (`:198`) and its async twin (`:214`).
   - I suggested a wording for this when R2 would be applied (S5 review, "Suggested rule update"). The draft was not
     changed before it was approved.
2. **R3, the guard:** "each client that calls a rule-10 default checks the object it is given once: a public method …
   without the interface that replaces it, or a public `Close()` on a serde that is not `IDisposable`, throws
   `IllegalArgumentException`".
   - The four mocks call the headers forms, which are rule-10 defaults, but they do not run the `Close()` check.
   - `SerdeGuard.CheckClose` throws only when `disposes` is true (`Internal/SerdeGuard.cs:67-73`; its comment says
     "The mocks never dispose a serde, so only the clients that do refuse a Close() they would not call").
   - The mocks pass `disposes: false`: `MockProducer.cs:72-73`, `AsyncMockProducer.cs:72-73`, `MockConsumer.cs:77-78`,
     `AsyncMockConsumer.cs:80-81`. Their constructor docs name only the headers form, which matches the code.
   - The plan limits this check to "the four real clients" (PLAN `:256`, `:1108`, and row 464 at `:1133`). The R3
     draft dropped that qualifier.
3. **R2, Encodings (minor):** "The String and UUID classes read Java's `*.serializer.encoding` keys".
   - The deserializer classes read `key.deserializer.encoding` / `value.deserializer.encoding`, then
     `deserializer.encoding` (`StringDeserializer.cs:43-44`).
   - The serializer classes fall back to the bare `serializer.encoding`.
   - None of those three keys matches `*.serializer.encoding`. The §4 Config row (`:924`) names them correctly.

**Why it matters.** This file is the Actors' rulebook.
- Points 1 and 2 state as rules two behaviours that the binding deliberately does not have. Both choices follow Java:
  Java's `MockProducer` passes fresh `RecordHeaders` on an empty cluster, and Java's mocks never close a serializer.
- An Actor checking code against these lines would "fix" the mock producers back to the record's own headers, which
  reverses 124-S4-1. Or it would add a `Close()` refusal that Java's mock does not have.

**Needs Pranav.** R1–R6 were applied word for word on his instruction (row 481), so only he can approve a rewording.

**What a fix must do.** Reword the three lines, then get approval:
- **R2, Headers:** "… The client tests the type once, at construction. `KafkaProducer` and `AsyncKafkaProducer` pass
  the record's own `Headers`, writable during the call and sealed after it. The mock producers pass a new, empty
  `Headers` to each call and drop it, as Java's `MockProducer` does on its empty cluster. The consumers pass the
  record's own."
- **R3:** "… or, on the four real clients (the mocks never dispose a serde), a public `Close()` on a serde that is not
  `IDisposable`, throws …"
- **Encodings:** name the keys: `key.` / `value.` + `serializer.encoding` or `deserializer.encoding`, then the bare
  key.

**Resolved:** a2c5ab7da (fixup! of b3c1f548d), with Pranav's approval of the three rewordings.
- The three sentences now match the code:
  - **Headers** (`dotnet/CLAUDE.md:712-716`): `KafkaProducer` and `AsyncKafkaProducer` pass the record's own `Headers`
    and seal them after the call (`SetReadOnly`). The mock producers pass a new, empty one to each call
    (`SerializedProducerRecord.cs:87-89`). The four consumers pass the record's own, through `ConsumerRecordsMarshal`.
  - **R3** (`:894-900`): the `Close()` refusal is limited to "the four real clients", which matches `disposes: false`
    on the four mocks.
  - **Encodings** (`:733-736`): the keys named are exactly the ones the four classes read
    (`StringSerializer.cs:45`, `StringDeserializer.cs:44`, `UUIDSerializer.cs:44`, `UUIDDeserializer.cs:43`).
- The diff touches only `dotnet/CLAUDE.md`: three hunks, 10 lines added and 7 removed, all inside those three
  sentences. Nothing else in the file changed.
- No new issue. Two lines are now unevenly wrapped (`:735`, `:899`), which is formatting only.

---
