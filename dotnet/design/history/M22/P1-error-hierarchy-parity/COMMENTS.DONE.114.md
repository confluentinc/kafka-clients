# Critic 114: resolved findings

## 1. The two mocks route a serializer's throw the way `KafkaProducer.doSend` does. Java's `MockProducer`, the core's mock and Python's mock all rethrow it unchanged.

**Files:** `dotnet/src/Confluent.Kafka/Producer/MockProducer.cs` and `AsyncMockProducer.cs`, the new
`catch (ApiException error) { return FutureFailure.Send/SendAsync(...); }` around
`SerializedProducerRecord.Serialize`. Also `SerializedProducerRecord.Serialize`'s
`InvalidCastException` wrap, which both mocks share with the real producers. The tests that pin it are
`PublicProducerSerializerFailureTests`'s `SyncMock` / `AsyncMock` cases.

**Reference:** Java `org.apache.kafka.clients.producer.MockProducer#send(ProducerRecord,Callback)`.
It checks closed, fenced and `sendException`. It then calls the serializers, either through
`partition(record, cluster)` or directly ("just to throw ClassCastException if serializers are not
the proper ones"). It has no `try` around them. So:

- an `ApiException` from a serializer leaves `send`: there is no callback and no `FutureFailure`;
- a `ClassCastException` leaves `send` unwrapped. The "Can't convert … specified in key.serializer"
  wrap is in `KafkaProducer#doSend` only.

The other two references already match Java:

- the Rust core mock, `rust/src/producer/mock_producer.rs` (`serialize_with_headers(..).await?`);
- Python's mock, `python/confluent_kafka/producer/_mock_core.py`, where the serializer call
  propagates.

**Problem:** on both .NET mocks, a serializer's `RecordTooLargeException` returns a failed future and
fires the callback on the caller's thread. A serializer's `InvalidCastException` becomes a
`SerializationException`. Java's mock throws both from `send` unchanged. The divergence is not
recorded as a deviation:

- `dotnet/CLAUDE.md`'s "A serializer's throw, as Java's `doSend`" bullet speaks of `doSend` only;
- STATUS.md lists the divergence only as "Open".

So it fails decision hygiene (§4), and it gives the mocks behaviour Java's mock does not have. A user
testing against `MockProducer` would see their serializer's `ApiException` arrive as a callback, where
Java's mock test sees it thrown.

**Fix (Java first; the core and Python already agree):**

- The two mocks call the serializers with no catch, so every throw (`ApiException`,
  `InvalidCastException`, anything else) leaves `Send` as the same instance.
  - Give `SerializedProducerRecord.Serialize` a parameter, or add a mock-only entry point, that
    skips the `InvalidCastException` wrap.
  - Drop the `catch (ApiException)` from `MockProducer.Send` and `AsyncMockProducer.Send`.
- Keep the D23 path on `KafkaProducer` and `AsyncKafkaProducer` only.
- Tests: in `PublicProducerSerializerFailureTests`, restrict the `ApiException_*`, `ACallbackThrow_*`
  and `InvalidCastException_*` theories to `SyncReal` / `AsyncReal`. Add mock theories asserting that
  the same `ApiException` and `InvalidCastException` instances are thrown from `Send`, with no
  callback and an empty history.
- Docs:
  - In `IProducer` / `IAsyncProducer`, add "on `KafkaProducer` / `AsyncKafkaProducer`" to the new
    remarks paragraph, and say that the mocks rethrow, as Java's mock does.
  - Do the same in `IDeliveryCallback`'s Thread and refusal paragraphs and in
    `IAsyncDeliveryCallback`'s summary.
  - Update `dotnet/CLAUDE.md`'s serializer bullet.
  - Remove the STATUS "Open" item.
  - In `ERROR-CATEGORIZATION.md` §6.3 and §10.6, replace "(all four producers)" and "The two mocks
    share the real producers' path".

**Resolution:** the two mocks rethrow every serializer throw unchanged, as Java's `MockProducer`.
- `SerializedProducerRecord.Serialize` takes `wrapInvalidCast`: `true` on `KafkaProducer` and
  `AsyncKafkaProducer` (the `InvalidCastException` wrap of Java's `doSend`), `false` on the mocks,
  which call the serializers with no `try`.
- `MockProducer` and `AsyncMockProducer` lose the `catch (ApiException)` and the
  `Common.Errors` using; `FutureFailure` is called only by the two real producers.
- `PublicProducerSerializerFailureTests` (36 cases): the `ApiException_*`, `ACallbackThrow_*` and
  `InvalidCastException_*` theories run on `SyncReal` / `AsyncReal` only, and the async-only ones on
  `AsyncReal`. `AnyOtherException_*` stays on all four. New mock theories: an `ApiException`, an
  `InvalidCastException` and a `KafkaException`, from the key or the value serializer, leave `Send`
  as the same instance, with no callback and an empty history, on both mocks; on
  `AsyncMockProducer` an `IAsyncDeliveryCallback` is not called either.
- Docs: `IProducer` / `IAsyncProducer` remarks and `SerializationException` texts,
  `IDeliveryCallback`'s Thread and which-records paragraphs, and `IAsyncDeliveryCallback`'s summary
  name the real producers and say the mocks rethrow. `dotnet/CLAUDE.md`'s serializer bullet and
  Serializers row, STATUS.md's entry (the "Open" item removed), and `ERROR-CATEGORIZATION.md`
  §6.3 and §10.6 are updated.

## 2. The rulebook still says the delivery callback never runs on the caller's thread

**Files:** `dotnet/CLAUDE.md`:

- the §3 idiom-map `Callback` row (about line 640): "⚠ **not the caller's task** … It fires on the
  producer's completion thread, on both surfaces";
- the API sketch comments (about lines 260 and 281): "Fires on the producer's completion thread";
- §4's delivery-callback **Thread** bullet (about line 885);
- §4's "A record rejected before the append fires nothing" (about line 902).

In `.claude/rules/ffi-marshalling.md`, the §A6 decision text lists the threads that run producer
callbacks.

**Problem:** since this change, `KafkaProducer.Send` / `AsyncKafkaProducer.Send` run the callback
inside `Send` on the caller's thread for a serializer's `ApiException`, as Java's `doSend` does. That
record is rejected before the append, yet it fires the callback. The new XML docs
(`IDeliveryCallback`) say so; the rulebook contradicts them. The Actor flagged the mismatch in
STATUS.md but left it open.

**Fix:** in each place, add the one exception: for a serializer's `ApiException`, the callback runs
once inside `Send` on the caller's thread, with Java's placeholder metadata, before the failed future
is returned (real producers only, after finding 1). This path never crosses the ABI (ffi §A6 form C),
so it has no `Callback_t`.

**Resolution:** the one exception is now stated, for `KafkaProducer` / `AsyncKafkaProducer` only:
for a serializer's `ApiException` the callback runs once inside `Send` on the caller's thread with
Java's placeholder metadata, before the failed future is returned, and nothing crosses the ABI.
- `dotnet/CLAUDE.md`: the two API sketch comments, the §3 serializer-throw bullet (which also says
  the mocks rethrow every serializer exception, as Java's `MockProducer`), the §3 `Callback` row,
  the Serializers row, the §4 **Thread** bullet, and §4's "A record rejected before the append
  fires nothing".
- `.claude/rules/ffi-marshalling.md` §A6: a paragraph after the list of threads that run producer
  callbacks names the one callback that runs on none of them and crosses no ABI.

## 3. ERROR-CATEGORIZATION.md §10 (untracked, not committed): numbering and layout

§10 jumps from item 6 (.NET) to item 11 (Python): items 7–10 are gone. There is also no blank line
before `**Python**`, so the heading is part of item 6's paragraph when the file is rendered.
Renumber, and add the blank line. Low severity: the file is not committed.

**Resolution:** §10 renumbered (Python 7–11, Rust / C 12–13), a blank line added before
`**Python**`, and §6.3 and §10.6 updated for the mock change. The file stays untracked.
