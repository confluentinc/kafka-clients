# COMMENTS.DONE.106 — M21/P2 (record headers)

## Pass 1 — `916ffb4e..4af0f2bd`

### 1. A header key containing U+0000 is silently truncated at the NUL (Low)

- **Commit:** `4af0f2bd` (S4).
- **Where:** `dotnet/src/Confluent.Kafka/Internal/Interop/ProducerSendMarshal.cs:225-258` (`AddHeader`,
  `WriteUtf8`).
- **What is wrong:** `AddHeader` encodes the key with `Encoding.UTF8.GetBytes`, which writes U+0000 as a
  0x00 byte, and hands the buffer to `kafka_common_header_Headers_add_with_key_value`, whose `key` is a
  NUL-terminated string (BH:27497-27500; the core reads it with `c_str_to_string`,
  `rust/src/ffi/common/header/mod.rs:357`). So `new Header("a\0b", v)` is sent as the key `"a"`, with no
  error. A probe against the built Release DLL (`ProducerSendMarshal.BuildRecord`, then the record's
  headers read back) gives `key=a len=1`.
- **Ground truth:** Java's `RecordHeader("a\0b", v)` sends the whole key (Java's `String` and the wire
  carry U+0000). Python rejects it: `producer_record_build_headers` takes each key through
  `str_utf8_borrowed`, which raises `ValueError("embedded null character")` because "a C string cannot
  carry" it (`python/_confluentkafka.c:312-325`, used at `:382`); `send()` raises, after the serializers
  and the seal (`python/confluent_kafka/producer/_base.py:546-556`). .NET silently sends another key
  than the one the record holds.
- **Fix:** reject a header key that contains U+0000 before the native call, as Python does, with
  `ArgumentException` (the type and text to rule, Python's being `ValueError("embedded null character")`;
  record it as a ledger row). A check in `AddHeader` (or one pass over `Entries` before the build) is
  safe on both sends: a throw out of `BuildRecord` destroys the builder and the headers in its `finally`,
  and both callers release the record's state (`NativeProducer.Send`'s `catch`, `AsyncDeliveryState.Send`'s
  `Abandon`). Add the case to `ProducerRecordBuildTests` or `PublicProducerSendHeadersTests`, on both
  Kafka producers and both mocks, asserting the type and message.
- **Note, not part of this item:** the topic has the same pre-existing truncation (`SetTopic`,
  `:208-222`, unchanged in behaviour by S4: `"t\0u"` is sent to `"t"`), which Python also rejects with the
  same `ValueError` (`_confluentkafka.c:425-426`). The same check could cover it if Pranav wants.

**Resolved:** `fee8336d` (`fixup!` of `4af0f2bd`), ledger 314 option (b). The topic and every header key
are checked by `ProducerSendMarshal.RejectAlteredStrings`, called first by `Send` and `SendWithHandOver` (all four
producers reach one of them through `NativeProducer`), before any pin or native call and after the serializers and the
Kafka producers' seal, as Python's `send()`; one scan, `AdminStrings.Validate(value, message, parameterName)`;
`ArgumentException` with `ParamName` "record" and "A topic must not contain …" / "A header key must not contain …". A
rejected record's headers stay sealed on the Kafka producers and writable on the mocks. Tests:
`PublicProducerSendStringTests` (33). Budgets unchanged.

## Pass 4 — `4c4ac59d`

### 1. The third async `Send` overload does not list two exceptions it throws

`dotnet/src/Confluent.Kafka/Producer/IAsyncProducer.cs:161-164` (`Send(record, IAsyncDeliveryCallback, ct)`). Its
`<exception>` list, which this commit extended, names `ArgumentNullException` and the new `ArgumentException`, but
not the `SerializationException` and `ObjectDisposedException` that the same path throws synchronously. Both
implementations call `SendValidated` (`AsyncKafkaProducer.cs:107` → `:118` `ThrowIfClosed`, `:121` `Serialize`;
`AsyncMockProducer.cs:114` → `:125`, `:128`), which is the path of the two sibling overloads. Those siblings list
both exceptions (`:103-104`, `:137-138`). This was already missing at `916ffb4e`, but the commit edited this block,
and check 4 asks that each `Send` doc match the code. Rule: ledger 38 (1) and the DoD's docs-match-code check.
Fix: add the two lines the siblings have, `SerializationException` "(thrown synchronously)" and
`ObjectDisposedException` "The producer is closed.".

**Resolved:** `f22963ac` (`fixup!` of `4c4ac59d`; its context lines are `4c4ac59d`'s `ArgumentException` lines, so it
squashes there). The block, now `IAsyncProducer.cs:161-166`, ends with the siblings' two lines in their order. The
three pairs (`:103-104`, `:137-138`, `:165-166`) are byte-identical: one hash for all three. The commit adds those 2
lines to that one file and changes nothing else (`--numstat` 2/0). The block matches the code: both implementations
(`AsyncKafkaProducer.cs:92-108`, `AsyncMockProducer.cs:99-115`) throw `ArgumentNullException` for `record` and
`callback`, then call the siblings' `SendValidated`, whose `ThrowIfClosed` throws `ObjectDisposedException`
(`NativeProducer.cs:499-505`) before `Serialize`.
