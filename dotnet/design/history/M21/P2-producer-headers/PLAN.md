# M21/P2 — producer record headers and the value semantics of `Header`, `Headers`, `ProducerRecord` (N=106)

**Status:** done (2026-11-04). Ruled 2026-11-04 (ledger 287–303, 305, 306, 312–317); squashed at
Pranav's request into one commit on a branch from master, PR #314. The evidence is the M21/P2 entry in
`dotnet/design/current/STATUS.md`.
**Split from** `dotnet/design/history/M21/P1-producer-metadata-and-headers/PLAN.md` by D11 (ledger
287). That plan's §2 (the fact refresh, ledger 277) and its cite prefixes apply here too.
**Mode:** A (.NET only). Q2 (a borrowed header value, ledger 305) and Q3 (the Python servers,
ledger 306) are follow-ups for later, not steps.
**Sources:** `A1-A3-HANDOFF.md` §2 A3; `FOLLOWUPS-REVIEW.md` §2 A3 and §9 R2 #2; ledger rows 98, 150,
167, 268, 269.

Cite prefixes: `src/` = `dotnet/src/Confluent.Kafka/`, `tests/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`grpc/` = `dotnet/grpc-server/`, `J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `JT/` =
`kafka/clients/src/test/java/org/apache/kafka/`, `py/` = `python/confluent_kafka/`, `pyt/` =
`python/test/`, `pyc` = `python/_confluentkafka.c`, `ffi/` = `rust/src/ffi/`, `BH` =
`rust/target/include/confluent_kafka_bindings.h`. Cites opened at `51f453af` on 2026-11-04 and
re-checked at `916ffb4e` (§1.1).

---

## 1. Scope

**In:** a mutable `Headers` sealed on send; headers on `ProducerRecord`; the send path through the
OptionsBuilder; the consumer's per-record `Headers`; the gRPC server's producer headers; R2 #2.
**Out:** serde headers (row 153); `MockProducer.history()` (A7); consumer zero-copy headers (row 152);
`Header` stays a sealed class.

### 1.1 Re-check at `916ffb4e` (P1 squashed and pushed)

- P1 (`51f453af..916ffb4e`) changed no Java, Python, Rust or C file, so every `J/`, `JT/`, `py/`,
  `pyt/`, `pyc`, `ffi/` and BH cite stands (BH unchanged since 2026-10-30). The .NET files P1
  touched that this plan cites: `src/Producer/ProducerRecord.cs`, `ConsumerRecordsMarshal.cs`,
  `grpc/Translate.cs`, `tests/RecordDeserializationExceptionTests.cs`, `tests/PublicProducerRecordTests.cs`,
  `dotnet/tests/Confluent.Kafka.IntegrationTests/KafkaProducerBrokerTests.cs`, `dotnet/CLAUDE.md`.
- Line drift: `tests/PublicProducerRecordTests.cs:105-107`, `:115-117` → `:189-191`, `:199-201`
  (D28, fixed in place); `dotnet/CLAUDE.md:344` → `:351`; the "Clipped to today's ABI" paragraph is
  `:299-309`, and P1's R2 already removed `RecordMetadata`'s accessors from it, so it lists
  `ProducerRecord.headers()` and `MockProducer.history()` only (`:304-305`). Unchanged and exact:
  `src/Producer/ProducerRecord.cs:59-64` (the constructor; `ToString` is `:117-125`),
  `ConsumerRecordsMarshal.cs:215`, `:224`, `:232`, `:235-247`, `grpc/Translate.cs:313-320`,
  `tests/RecordDeserializationExceptionTests.cs:91`, `Headers.cs`, `Header.cs`, `KafkaProducer.cs`,
  `AsyncKafkaProducer.cs`, `NativeProducer.cs`, `ProducerSendMarshal.cs`, the budget tests and
  `PublicHeadersConstructorTests.cs:42`.
- Added: `kafka_producer_ProducerRecordOptions_destroy_err` is BH:52925; `set_topic_err` copies its
  topic (`ffi/producer/producer_record.rs:622-632`).
- No ruling rests on a fact that P1 changed. Nothing in P1 added a `Headers.Count` / indexer use,
  a reference to `Headers.Empty` or a new `ProducerRecord` construction path.

## 2. Decisions (all ruled; ledger rows in brackets)

| D | Ruling | Detail (Java / Python cites) |
|---|---|---|
| D12 [288] | Java's `Headers` members | `Add(Header)`, `Add(string, byte[]?)`, `Remove(string)`, `LastHeader(string)`, D13's lookup, `ToArray()` (`Header[]`, `Array.Empty<Header>()` when empty); `Add` / `Remove` return the `Headers`. Java's order: `add(null)` checks the header before the closed check; `remove` checks the closed state before the key; `lastHeader` / `headers` check the key (`J/common/header/internals/RecordHeaders.java:63-105`, `:124-132`; Python `py/common/header/headers.py:119-172`, `:255-260`). Texts per D18 and ledger 286. |
| D13 [289] | (a) | `IEnumerable<Header> GetHeaders(string key)`: a lazy iterator (Java's live view, `RecordHeaders.java:101-105`); the null key checked at the call (an eager check plus a private iterator). CS0542 rules out `Headers(string)`. |
| D14 [290] | `IEnumerable<Header>` only | `Count` and the indexer go (`src/Common/Header/Headers.cs:26`, `:82`, `:85`); one test use (`tests/PublicHeadersConstructorTests.cs:42`). Java `Iterable`; Python `__iter__` only. |
| D15 [291] | internal read-only state | internal `SetReadOnly` / `IsReadOnly` (`volatile bool`); `Add` / `Remove` throw `InvalidOperationException("RecordHeaders has been closed.")` (`RecordHeaders.java:112-132`; Python `headers.py:47`, `:199-213`; ledger 14). Reads still work. |
| D16 [292] | seal in the Kafka* producers | `KafkaProducer` / `AsyncKafkaProducer` `SendValidated`, after `Serialize`, before the native call (`src/Producer/KafkaProducer.cs:91-94`, `AsyncKafkaProducer.cs:120-123`), as Python (`py/producer/_base.py:552`); a serializer throw leaves them writable; a resent record works; `MockProducer` / `AsyncMockProducer` never seal, as Java's (`J/clients/producer/MockProducer.java:614-615`) and Python's (`pyt/unit/test_headers.py:620-629`). Java's `testHeadersFailure` is skipped with Python's reason (`pyt/unit/test_kafka_producer.py:85-89`). |
| D17 [293] | per-record fetched headers | no shared `Headers.Empty` (`Headers.cs:28`; `src/Internal/Interop/ConsumerRecordsMarshal.cs:215`, `:224`, `:232`): each record gets its own writable `Headers`; `Wrap` takes the owned per-record `List<Header>` (`:235-247`) and stays non-copying; `tests/RecordDeserializationExceptionTests.cs:91` moves off `Headers.Empty`. |
| D18 [294] | Java's texts | `Header`'s null key → `ArgumentNullException(nameof(key), "Null header keys are not permitted")` (`RecordHeader.java:33`); `Add(null)` → `ArgumentNullException(nameof(header), "Header cannot be null.")` (`RecordHeaders.java:65`); a null key in `Remove` / `LastHeader` / `GetHeaders` → `ArgumentNullException(nameof(key), "key cannot be null.")` (ledger 286). |
| D19 [295] | Java's value semantics | `Header`: equality on key and value bytes, hash `31 * key.hashCode + content hash`, `RecordHeader(key = k, value = [1, -1])` with signed bytes and `null` (`RecordHeader.java:67-89`). `Headers`: ordered element equality, the read-only state not compared, `RecordHeaders(headers = [...], isReadOnly = false)` with Java's lowercase `false` (`RecordHeaders.java:153-177`). Python: `py/common/header/header.py:99-126`, `headers.py:177-195`. |
| D20 [296] | (a) | `ProducerRecord(topic, value, key = default, partition = null, timestamp = null, headers = null)` (`src/Producer/ProducerRecord.cs:59-64`). |
| D21 [297] | (a) | `IEnumerable<Header>? headers` (a `Headers`, a `Header[]` or a list). |
| D22 [298] | the record's own copy | `public Headers Headers { get; }`, built through `Headers(IEnumerable<Header>?)` (`Headers.cs:53-74`): `null` → empty, a `Headers` copied (not its read-only state), a `null` element → `ArgumentNullException("Header cannot be null.")` (`J/clients/producer/ProducerRecord.java:85`, `:158`; Python `py/producer/producer_record.py:184`, `:190-193`). |
| D23 [299] | (i) | one `Headers` object per record, its `List<Header>` allocated on the first `Add` or from non-empty input. The send budgets build the record inside the measured window (`tests/PublicProducerSendAllocationBudgetTests.cs:151`, budget `:52`; `tests/PublicSyncProducerSendAllocationBudgetTests.cs:193`, `:209`, budget `:91`): re-measure; a constant that would move is reported to Pranav with the measured delta before it moves. |
| D24 [300] | (c) | the OptionsBuilder for **every** record, as Python's `producer_record_to_c` (`pyc:2268-2323`, used by both sends at `:2351`, `:2450`): `ProducerRecordOptionsBuilder_new`, `set_topic`, `set_partition`, `set_timestamp`, `set_key`, `set_value`, then, only when the sealed headers are non-empty, `RecordHeaders_new`, `__as_Headers`, `Headers_add_with_key_value` per header and `set_headers`; `build`, the builder's destroy, the headers' destroy, `ProducerRecord_with_options`, the options' destroy. `ProducerRecord_with_partition_timestamp_key` is no longer used. Key and value keep their pins (`kafka_Bytes_t *`, borrowed until the sync call returns or the async handover, `ffi/producer/producer_record.rs:676-720`); the header bytes are copied by `Headers_add_with_key_value` inside the record build, on the caller's thread for both sends (`src/Internal/NativeProducer.cs:220`, `:246`), so no header pin outlives the call. The transients are freed in `finally` (Python `pyc:2309-2317`). The per-send P/Invoke count goes from 2 (build, destroy) to 11 without headers, as Python's 10 plus the record's destroy: Pranav's no-header A/B (ledger 29) gates it. |
| D25 [301] | recorded deviation | the header-value copy (no borrowed form in the C API; three native copies, P1 PLAN §2.1 #7) is a deviation from root `CLAUDE.md` §14, as Python's; the managed send adds no allocation per header (the key encoded on the stack up to a bound, else pinned; the value under `fixed`); an empty value crosses as a non-null pointer (the ffi §A4 sentinel), a `null` value as `NULL` data. |
| D27 [302] | (a) | `grpc/Translate.cs:313-320` `ProducerRecordFromProto` passes the proto's headers (`rust/multilanguage-test-server/proto/producer_service.proto:127`, value `bytes`, never null, `:132`), as C's server (`c/grpc_server/server.cc:910-955`). |
| D28 [303] | Java's | `ProducerRecord.Equals` / `GetHashCode` over topic, partition, headers, key, value, timestamp (key and value through `EqualityComparer<T>.Default`: a `byte[]` by reference, as Java's `Objects.equals`) and `ToString` "ProducerRecord(topic=…, partition=…, headers=…, key=…, value=…, timestamp=…)" (`ProducerRecord.java:190-226`; Python `producer_record.py:212-229`). Two `ToString` tests change (`tests/PublicProducerRecordTests.cs:189-191`, `:199-201`). |

## 3. Steps (one commit each)

- **S0 — rules** (markdown only; path check, ledger 58). Drafts R1–R16 in §3.1, applied verbatim
  (approved, ledger 312; R13 and R15 edit the critic persona, re-copied to the root discovery copy).
- **S1 — `Header` / `Headers` value semantics** (D18, D19). New `tests/PublicHeadersTests.cs`:
  Python's `test_header_null_key_is_javas_requires_non_null`, `test_header_equality_hash_and_string_are_record_headers`,
  `test_headers_equality_hash_and_string_are_record_headers` (`pyt/unit/test_headers.py:326`, `:337`, `:431`).
- **S2 — a mutable `Headers`** (D12–D15, D17, D23's lazy list). Tests: every `@Test` of
  `JT/common/header/internals/RecordHeadersTest.java` in Java order (`:44` … `:262`), as Python's
  `test_headers.py:75-318`: `testHeadersIteratorRemove` (`:174`) and `testReadOnly`'s two
  `iterator.remove()` asserts are not applicable (a .NET enumerator has no `Remove`), said in the
  class; `testNew` (`:238`) in full; the two `@RepeatedTest(100)` read-safety tests (`:295`, `:304`) as
  loops of 100 with 16 threads (Python `:308`, `:315`). Python's `test_headers_null_key_checks_and_order`,
  `test_headers_add_forms_and_chaining`, `test_record_headers_never_none`,
  `test_extension_builds_a_writable_headers_per_record`, `test_poll_hands_out_writable_headers_after_the_deserializers`
  (`:368`, `:386`, `:601`, `:648`, `:658`), the last two through `MockConsumer`. The consumer budgets
  re-measured (`tests/PublicSyncConsumerAllocationBudgetTests.cs:46`, `PublicConsumerTypedAllocationBudgetTests.cs:47`).
- **S3 — `ProducerRecord` headers and value semantics** (D20–D23, D28). Tests: Java's
  `testEqualsAndHashCode` (`JT/clients/producer/ProducerRecordTest.java:28`); Python's
  `test_equals_and_hash_code` and `test_producer_record_forms_and_headers` as far as .NET has the
  forms (`pyt/unit/test_producer_types.py:39`, `:70`); a `Headers` argument is copied, not shared or
  sealed; `ToString` in Java's order; the send budgets re-measured.
- **S4 — the send path and the seal** (D16, D24, D25). Imports (the `_err` twins where BH has them):
  `kafka_common_header_internals_RecordHeaders_new_err` (BH:27699), `_RecordHeaders__as_Headers_err`
  (BH:27825), `kafka_common_header_Headers_add_with_key_value` (BH:27497), `_RecordHeaders_destroy_err`
  (BH:27840), `kafka_producer_ProducerRecordOptionsBuilder_new_err` … `_destroy_err` (BH:52764-52912),
  `kafka_producer_ProducerRecord_with_options` (BH:52618), `kafka_producer_ProducerRecordOptions_destroy_err` (BH:52925). Tests: Java's
  `testHeadersSuccess` (`JT/clients/producer/KafkaProducerTest.java:1078`; Python's `test_headers_success`,
  `pyt/unit/test_kafka_producer.py:298`) on `KafkaProducer` and `AsyncKafkaProducer` over an
  unreachable bootstrap; Python's `test_mock_producer_leaves_the_headers_writable` (`test_headers.py:620`)
  on both mocks; an Interop test that builds the native record and reads its partition, timestamp,
  key, value and headers back (a null value, an empty value, a non-ASCII key; the record with no
  headers too); no managed allocation per header (0 vs 4 headers); the send budgets re-measured after
  the OptionsBuilder; a broker test in `dotnet/tests/Confluent.Kafka.IntegrationTests/KafkaProducerBrokerTests.cs`
  (Python's `pyt/integration/test_kafka_producer_broker.py:415-441`).
- **S5 — gRPC producer headers** (D27). A `TranslateTests` case.
- **S6 — comment-only commit** (ledger 59); **S7 — the STATUS entry**.

### 3.1 S0 rule drafts (written at `916ffb4e`; each replaces exactly the text quoted)

**R1 — `dotnet/CLAUDE.md` §3, the `ProducerRecord` block of the producer sketch (`:201-209`)** becomes:
```csharp
public sealed class ProducerRecord<TKey, TValue> {   // Java `ProducerRecord<K,V>`
    public string Topic { get; }
    public int? Partition { get; }               // null = let the producer choose
    public long? Timestamp { get; }              // null = producer stamps it
    public TKey? Key { get; }                    // null (ref) = no key; three-state via the serializer (§4)
    public TValue? Value { get; }                // null (ref) = tombstone; three-state via the serializer (§4)
    public Headers Headers { get; }              // the record's own copy; read-only once a KafkaProducer sends it
    public ProducerRecord(string topic, TValue? value,
        TKey? key = default, int? partition = null, long? timestamp = null,
        IEnumerable<Header>? headers = null);   // Java's checks, topic → timestamp → partition
    // Equals, GetHashCode and ToString are Java's (topic, partition, headers, key, value, timestamp)
}
```

**R2 — `dotnet/CLAUDE.md` §3, "Clipped to today's ABI" (`:299-309`).** Replace
"`send(record, Callback)` now goes through the ABI's per-record `Callback_t`, ffi §A6, decision 10):
`ProducerRecord.headers()`,
and `MockProducer.history()` (Java returns" with
"`send(record, Callback)` now goes through the ABI's per-record `Callback_t`, ffi §A6, decision 10;
`ProducerRecord.headers()` shipped in M21/P2): `MockProducer.history()` (Java returns".

**R3 — `dotnet/CLAUDE.md` §3, "The typed serialize path" (`:311-315`).** Replace
"the serialized bytes flow into the record that
`ProducerRecord_new*` builds (an internal `SerializedProducerRecord` carrier)." with
"the serialized bytes and the record's sealed headers flow into the record that
`ProducerRecord_with_options` builds through the OptionsBuilder, for every record, as Python's
binding builds it (ledger 300; an internal `SerializedProducerRecord` carrier)."

**R4 — `dotnet/CLAUDE.md` §3, the consumer sketch (`:351`).** Replace
"`public Headers Headers { get; }  // materialized owned bytes (not routed through a deserializer this phase)`" with
"`public Headers Headers { get; }  // the record's own, writable copies (not routed through a deserializer this phase)`".

**R5 — `dotnet/CLAUDE.md` §3 (`:545-546`).** Replace
"typed *headers* on `ConsumerRecord<K,V>` (they are materialized
owned bytes today)." with
"headers for the deserializers (row 153; each `ConsumerRecord`'s `Headers` are its own, writable
copies today, ledger 293)."

**R6 — `dotnet/CLAUDE.md` §3, the idiom map, a new row after the `` `byte[]` key/value `` row (`:628`):**
"| `Headers` / `Header` (record headers) | `Headers : IEnumerable<Header>` with Java's `Add` / `Remove` / `LastHeader` / `ToArray` and `GetHeaders(key)` for Java's `headers(String)` (CS0542); read-only once a `KafkaProducer` sends the record (`InvalidOperationException("RecordHeaders has been closed.")`), never on the mocks; a fetched record's are its own and writable | send: the header values are copied by the ABI, which has no borrowed form, a recorded deviation from root `CLAUDE.md` §14 (ledger 301) — ffi §A4 |"

**R7 — `dotnet/CLAUDE.md` §1, "One call, end to end" (`:36`).** Replace
"`   validate args · serialize · pin key/value · Callback_new (self = the record's state)`" with
"`   validate args · serialize · seal headers · pin key/value · build the record (OptionsBuilder) · Callback_new (self = the record's state)`".

**R8 — `dotnet/.claude/rules/ffi-marshalling.md` §A2, Category 2 (`:457-458`).** Replace
"the per-send `ProducerRecord_t`, freed right after
the send call (`ProducerRecord_destroy_err`, BH:39731);" with
"the per-send `ProducerRecord_t`, freed right after
the send call (`ProducerRecord_destroy_err`, BH:39731); the per-send
`ProducerRecordOptionsBuilder_t`, `ProducerRecordOptions_t` and, for a record with headers,
`RecordHeaders_t` that build it, each freed in a `finally` once the record is built;"

**R9 — `dotnet/.claude/rules/ffi-marshalling.md` §A2, the table: three rows after the
`` `ProducerRecord_t` `` row (`:478`):**
"| `ProducerRecordOptionsBuilder_t` | 2 | the binding, per send (`ProducerRecordOptionsBuilder_new_err`, BH:52764) | the binding, after `build` (`ProducerRecordOptionsBuilder_destroy_err`, BH:52912) |
| `RecordHeaders_t`, for a record with headers | 2 | the binding, per send (`RecordHeaders_new_err`, BH:27699) | the binding, after `set_headers_err` copied it (`RecordHeaders_destroy_err`, BH:27840) |
| `ProducerRecordOptions_t` | 2 | `ProducerRecordOptionsBuilder_build` (BH:52899) | the binding, after `ProducerRecord_with_options` copied it (`ProducerRecordOptions_destroy_err`, BH:52925) |"

**R10 — `dotnet/.claude/rules/ffi-marshalling.md` §A4, the topic bullet (`:683-685`)** becomes:
"  - **The topic is read when the record is built.** `ProducerRecordOptionsBuilder_set_topic_err`
    copies its NUL-terminated `topic` (`rust/src/ffi/producer/producer_record.rs:626-628` at `916ffb4e`), so the topic needs only a call-scoped buffer (on the
    stack up to a bound, else `Utf8Marshal.Pin`) around that call, on both paths. No interned topic cache.
  - **Every record is built through the OptionsBuilder** (ledger 300), as Python's binding builds it:
    `ProducerRecordOptionsBuilder_new_err`, `set_topic` / `set_partition` / `set_timestamp` (-1 is Java's
    `null`) / `set_key` / `set_value` (`NULL` is absent), then, only when the record's sealed headers are
    non-empty, `RecordHeaders_new_err`, `__as_Headers_err`, `Headers_add_with_key_value` per header and
    `set_headers_err`; then `build` and `ProducerRecord_with_options`. The builder and the record hold
    the key's and value's `kafka_Bytes_t *`, never their bytes, so the pins above are unchanged.
  - **Header bytes are copied during the build.** `Headers_add_with_key_value` copies the key and the
    value (BH:27497), so each needs only a call-scoped pin (`fixed`; the key's UTF-8 on the stack up to
    a bound), on both sends: nothing of a header outlives the record build, and the managed send
    allocates nothing per header. A `null` value crosses as `NULL` data; an empty value as a non-null
    pointer with length 0 (the sentinel byte above: `fixed` over an empty array yields `NULL`, which the
    ABI reads as a `null` value). The copy is a recorded deviation from root `CLAUDE.md` §14 (ledger 301):
    the C API has no borrowed header value (ledger 305), as for Python's binding."

**R11 — `dotnet/.claude/rules/ffi-marshalling.md` §A4, Anti-patterns (after `:697`)**, a new bullet:
"  - A header value passed through `fixed` over an empty array (a `NULL` pointer, read as a `null`
    value); a header pin, or a managed copy of a header (`ToArray()` per send), kept past the record build."
**and Tests required (after `:714`)**, a new bullet:
"  - A record with headers sends them: a `null` value, an empty value and a non-ASCII key read back
    from the native record; a send with headers adds no managed allocation per header."

**R12 (optional) — `dotnet/.claude/rules/bindings.md` §2.5 (`:94-96`).** After "and copy only when
lifetime forces it." add " (or when the C ABI has no borrowed form: the producer's header values,
which the .NET and Python bindings copy, `dotnet` ledger 301)."

**R13 (persona) — `dotnet/.claude/agents/dotnet-critic.md:20`.** Applied together with R15: see
R15 for the line's whole new text.

**R14 — the `ProducerRecord_t` mirror struct, which does not exist** (the record is an opaque handle
the binding builds and destroys per send; the only `[StructLayout]` struct is `KafkaBytes`,
`src/Internal/Interop/KafkaBytes.cs:23-24`).
- **R14a — `dotnet/.claude/rules/ffi-marshalling.md` §0.1, the type map (`:77`).** Replace
  "| `ProducerRecord_t` | `[StructLayout(LayoutKind.Sequential)] struct`, array `[]` | Fixed-width ⇒ identical layout. |" with
  "| `kafka_Bytes_t` (`const uint8_t *data`, `int32_t len`) | `[StructLayout(LayoutKind.Sequential)] struct` (`KafkaBytes`) | Fixed-width ⇒ identical layout. A record (`ProducerRecord_t`) and its builder are opaque `*_t *` handles, not mirrored (§A2). |"
- **R14b — `dotnet/CLAUDE.md` §2 (`:173-175`), the same stale fact** (folded in with R14a; strike it
  if not wanted). Replace
  "and the blittable
  `[StructLayout]` mirror structs (e.g. the `ProducerRecord_t` mirror — the
  interop twin of the public `ProducerRecord`)." with
  "and the blittable
  `[StructLayout]` structs the ABI passes by value (`KafkaBytes`, the ABI's `kafka_Bytes_t`); a
  record (`ProducerRecord_t`) is an opaque handle, not mirrored."

**R15 (persona) — `dotnet/.claude/agents/dotnet-critic.md:20`, with R13.** Replace
"- **Pinning & zero-copy (ffi §A4 send / §B4 receive):** send key/value pinned **call-scoped**, no intermediate copy, never held past the send call; receive borrows into the batch — copy-out before `_destroy`, no stored native-backed `ReadOnlyMemory`." with
"- **Pinning & zero-copy (ffi §A4 send / §B4 receive):** send key/value pinned with no intermediate copy, for the sync send's call and, on the async send, until the handover `cb`, never past it or across the delivery future; the producer's header values are copied by the ABI during the record build, a recorded deviation (ledger 301), not a finding; receive borrows into the batch — copy-out before `_destroy`, no stored native-backed `ReadOnlyMemory`."

**R16 — `dotnet/.claude/rules/ffi-marshalling.md` §A5.**
- The Precondition bullet (`:740-742`): replace
  "`ArgumentOutOfRangeException` (a **negative partition** — the ABI silently
    maps negative to "unset", so the binding must reject it — or a negative
    timeout)," with
  "`ArgumentOutOfRangeException` (a **negative partition** or a **negative
    timestamp** — the ABI silently maps either to "unset", so the binding must reject it,
    with Java's texts, ledger 284 — or a negative timeout),".
- Tests required: replace "negative partition → `ArgumentOutOfRangeException` —" with
  "negative partition or timestamp → `ArgumentOutOfRangeException` with Java's text —".

**R17 — `dotnet/.claude/rules/ffi-marshalling.md` §A4, the pinning-primitive bullet (`:651-653`; written at
`fee8336d`; approved with R17b, ledger 316).** The passage names two things that no longer exist: there is no interned
topic buffer (R10's bullet, `:688-690`), and the async empty sentinel is not a pinned managed byte but one
process-wide byte of native memory (`Marshal.AllocHGlobal(1)`, `dotnet/src/Confluent.Kafka/Internal/Interop/SendPins.cs:27`,
never moved or freed). Replace
  "`GCHandle.Alloc(Pinned)` remains correct for
    buffers the binding *owns* and pins itself — the interned topic buffers and the
    empty-sentinel byte below." with
  "`GCHandle.Alloc(Pinned)` remains correct for
    buffers the binding *owns* and pins itself — the UTF-8 buffer `Utf8Marshal.Pin` encodes
    for a topic or header key too long for the stack. The async empty sentinel below needs no
    pin: it is one process-wide byte of native memory (`Marshal.AllocHGlobal(1)`), never moved
    or freed."

**R17b — `dotnet/.claude/rules/ffi-marshalling.md` §A4, the async sentinel warning (`:683-684`; written after
Pranav's "fix the wording", ledger 316).** Replace
  "**one process-wide, permanently pinned
    1-byte static** instead" with
  "**one process-wide byte of native memory, never moved
    or freed** (`Marshal.AllocHGlobal(1)`, `SendPins`) instead".

## 4. Gates

As P1 PLAN §6. Pranav's no-header perf A/B (ledger 29), each against `916ffb4e`: checkpoint 1 at
the S4 commit (the first commit with the OptionsBuilder send and the per-record `Headers`, which S3
adds); checkpoint 2 at P2's last commit before the STATUS entry (after S5, the comment-only commit
and any fixups), which differs from checkpoint 1 only where a later commit touched the send path.

## 5. Rulings

Pranav, 2026-11-04, verbatim (three replies to the coordinator's summaries):
1. "D4: (b) / D11: Two phases / D13: (a) / D20: (a) / D21: (a) / D23: (i) / D24: (c) (If python does exactly the same) / D27: (a) / D5: Use what java has / D10: ArgumentNullException and ArgumentOutOfRangeException carrying Java's texts / D18: Java's way / Q1, Q2, Q3 for later"
2. "group b table : agree / D24 (c) : Lets do python way / D5: (b) / D4(b) : (i)"
3. "#1 its a yes on 9 items / #2 (3) / #3 record R3d as answered"

Recorded as ledger rows 277–307 (this phase: 287–303, 305, 306).

Pranav, 2026-11-04, on the S0 drafts, verbatim: "Approve R1-R11, R12 & R13 yes, Fold in three out of date passages."
Ledger 312: R1–R13 approved, R10 citing `rust/src/ffi/producer/producer_record.rs:626-628` at `916ffb4e`
for the topic copy (BH:52781 does not say it); R14–R16 (the three out-of-date passages, written
after the ruling) folded in; R14b is the same stale fact in `dotnet/CLAUDE.md` §2, shown to Pranav with
R14–R16 for any fixup.

Pranav, 2026-11-04, on the S3 budget stop and R14–R16, verbatim: "Rule on budget (a). R14-R16 agree."
Ledger 313: D23 (i) costs 40 B per send (the record's `Headers`, 32 B, and the record's growth from
64 to 72 B); both async send fast-path budgets move 440 → 480 (measured 464 B + 16 B) in S3's commit,
`PublicProducerSendAllocationBudgetTests.cs:52` and `PublicProducerDeliveryCallbackAllocationBudgetTests.cs:58`;
the other budgets stay. R14–R16 stand as applied in `ec1970b2`.

Pranav, 2026-11-04, on Critic 106 finding 1 (a NUL or an unpaired surrogate cut from a header key or
the topic), verbatim: "lets go with option (b)". Ledger 314: header keys and the topic are rejected
with `ArgumentException`, worded as `Internal/AdminStrings.cs`, from the shared send path on all four
producers, where Python's `send()` raises; two recorded deviations at the site (Java sends both whole;
Python's mocks accept them). A `fixup!` of `4af0f2bd`.

Pranav, 2026-11-04, on the perf A/B (`916ffb4e` vs `fee8336d`) and R17, verbatim: "I think we can go
with (a). Regarding R17 fix the wording." Ledger 315: D24 (c) stands, its measured cost (paced 400k:
throughput unchanged, CPU +10 / +13 points, latency unchanged; max rate −2.8 % sync, −1.3 % async) in the
STATUS entry. Ledger 316: R17 as drafted plus R17b (`:683-684`), one `fixup!` of `ec1970b2`. Ledger 317
corrects row 314's premise: Java sends a NUL whole but writes `?` for an unpaired surrogate
(`Utils.utf8`); option (b) stands. `PartitionsFor(topic)`'s NUL truncation is unruled, listed as open.
