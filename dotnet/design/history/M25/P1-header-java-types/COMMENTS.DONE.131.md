# Critic 131: M25/P1 resolved items

### 131-S2-1 (Low, test-coverage): no test pins the `"null"` that `Headers.ToString` prints for an element whose `ToString()` returns null

- **Where**: `dotnet/src/Confluent.Kafka/Common/Header/Headers.cs:259`
  (`builder.Append(headers[i].ToString() ?? "null");`).
- **What**: the branch is correct, and new in this commit: only a user `IHeader` can return null from
  `ToString()`, since `Header` never does. No test reaches it. The only element text a test checks for a user
  header is `EqualityHeader(k)` (`PublicUserHeaderTests.cs:237`), and no header test double has a
  `ToString()` that returns null (`git grep 'ToString() => null' -- dotnet/tests` finds only
  `ClientConfigsTests.NullText`). If someone drops `?? "null"`, `StringBuilder.Append((string?)null)` appends
  nothing, the output becomes `RecordHeaders(headers = [], isReadOnly = false)` for one such header, and the
  suite still passes.
- **Evidence**: Java's `org.apache.kafka.common.header.internals.RecordHeaders#toString` concatenates
  `headers`, an `ArrayList`. `java.util.AbstractCollection#toString` appends each element through
  `StringBuilder.append(Object)`, which is `String.valueOf(obj)`, and appending a null `String` writes
  `null`. So Java prints `RecordHeaders(headers = [null], isReadOnly = false)`.
- **Fix**: in `PublicUserHeaderTests`, add a user header whose `ToString()` returns null, and assert
  `new Headers(new IHeader[] { it }).ToString()` is `"RecordHeaders(headers = [null], isReadOnly = false)"`.

- **Fixed in 6ddf96738**: `PublicUserHeaderTests.HeadersToString_PrintsNullForAUserHeaderWhoseToStringIsNull` asserts `RecordHeaders(headers = [null, EqualityHeader(k)], isReadOnly = false)` for a user header whose `ToString()` returns null (red with `?? "null"` dropped).

### 131-S2-2 (Low, docs + test-coverage): a header key that turns invalid at its second read is rejected after the async send's pin; the comment still says nothing is pinned yet, and this failure path has no pin-balance check

- **Where**: `dotnet/src/Confluent.Kafka/Internal/Interop/ProducerSendMarshal.cs:105-107` (the
  `RejectAlteredStrings` comment: "the string is rejected before anything is pinned"), `:85-86`
  (`RejectAlteredStrings`, then `pins.Pin`), `:264` (`AddHeader` now calls `HeaderKey`, inside
  `BuildRecord`); test `dotnet/tests/Confluent.Kafka.UnitTests/PublicUserHeaderTests.cs:146`.
- **What**: the commit adds a check at the second key read, in `AddHeader`. On `SendWithHandOver` that read
  happens after `pins.Pin`. A user header whose key is valid at the first read and null or NUL-containing at
  the second now throws with the key and value pinned, inside `BuildRecord`, while the builder and the
  native `RecordHeaders_t` are live (the `finally` of `BuildRecord` frees both) and before
  `state.Register()`. The code handles this: `AsyncDeliveryState.Send`'s `catch { Abandon(); throw; }`
  releases the pins and the `GCHandle`. Two things are now out of date:
  1. The comment at `:105-107` is no longer true for header keys. It holds only for the first read.
  2. `ffi-marshalling.md` §A4 "Tests required" asks for "pin/unpin balance on every path, including the
     failure paths". The test for this path (`Send_AUserHeaderWhoseKeyIsNullOnlyAtALaterRead_…`, run on
     `AsyncKafka` and `AsyncMock`) serializes strings to fresh arrays and checks no pin count. The existing
     post-pin test (`ProducerSendPinLifetimeTests.AsyncSend_FailingAfterThePin_UnpinsEachBufferOnce`, through
     `SendOnAClosedProducer`) throws later, at the P/Invoke's `AddRef`, after the record was built and
     `Register` ran. So it does not cover a throw from inside `BuildRecord`.
- **Evidence**: before this commit, nothing inside `BuildRecord` could throw a managed exception for a valid
  record, because `Header.Key()` / `Value()` are sealed field reads and the key had already been checked.
  The user `IHeader` makes this throw site new.
- **Fix**: reword the comment, for example "the topic and each header key's first read are rejected before
  anything is pinned; `HeaderKey` checks every later read". This can go in the phase's comment-only commit
  (`dotnet/CLAUDE.md` §4 Comments, rule 8). Then add the check, either in `ProducerSendPinLifetimeTests` or as
  an async variant of the later-read test: send a record whose key and value are `NativeMemoryManager`
  buffers (`MemorySerializer`) and whose user header answers `"k"` and then `null`, on `AsyncMockProducer`.
  Assert Java's `IllegalArgumentException` and `key.PinCount == key.UnpinCount == 1` (the same for the value).

- **Fixed in 6ddf96738**: the `RejectAlteredStrings` comment says only the topic and each header key's first read are rejected before the pin, `HeaderKey` checking every later read; `PublicUserHeaderTests.AsyncMockSend_AUserHeaderWhoseKeyIsNullOnlyAtALaterRead_UnpinsTheKeyAndValueOnce` asserts Java's `IllegalArgumentException` and `PinCount == UnpinCount == 1` for the `NativeMemoryManager` key and value on `AsyncMockProducer` (red with `Abandon()` dropped from `AsyncDeliveryState.Send`'s catch).

### 131-S3-1 (Low, efficiency): every `RecordHeader` grew from 32 B to 48 B for the buffer form, which no binding path uses; the consumer pays it once per received header

- **Where**: `dotnet/src/Confluent.Kafka/Common/Header/Internals/RecordHeader.cs:35-39` (four fields). The
  per-header callers all use the `(string, byte[]?)` form: `Internal/Interop/ConsumerRecordsMarshal.cs:341`
  (one per received header), `RecordHeaders.cs:114` (`Add(string, byte[]?)`), `dotnet/grpc-server/Translate.cs:468`.
- **What**: Java's class has four fields because Java's consumer builds its headers through the buffer form, where
  all four are used. The .NET consumer copies each header out before `ConsumerRecords_destroy` (`dotnet/CLAUDE.md`
  §6.4, copy-out) and always builds the `(string, byte[]?)` form, so `_keyBuffer` and `_valueBuffer` stay null for
  the header's whole life: 16 B of unused fields per received header. Root `CLAUDE.md` §13 asks the per-record
  paths to account for such costs. No budget test sees it: `PublicSyncConsumerAllocationBudgetTests` polls records
  without headers, and `PublicProducerHeadersAllocationTests` builds its headers outside the measured region.
- **Evidence**: in a scratch copy of `5319d8424`, `GC.GetAllocatedBytesForCurrentThread` over 1000
  `new RecordHeader("k", v)` on net10.0 gives 48 B per header (the parent's `Header`: 32 B). A received header
  with an 8-char key and a 16-byte value owns its key string (40 B), its value array (40 B), the header and a list
  slot (8 B): 120 B before this commit, 136 B after it (+13%).
- **Fix**: keep Java's behaviour in two fields, each holding the value once read or its pending buffer before:
  ```csharp
  // A string, or the key buffer until the first read decodes it.
  private volatile object _key;
  // Null, a byte[], or the value buffer until the first read copies it.
  private volatile object? _value;

  public string Key()
  {
      if (_key is string key)
      {
          return key;
      }

      lock (this)
      {
          if (_key is StrongBox<ReadOnlyMemory<byte>> buffer)
          {
              _key = JavaCharset.Utf8.Decode(buffer.Value.Span);
          }

          return (string)_key;
      }
  }

  public byte[]? Value()
  {
      object? value = _value;
      if (value is not StrongBox<ReadOnlyMemory<byte>>)
      {
          return (byte[]?)value;
      }

      lock (this)
      {
          if (_value is StrongBox<ReadOnlyMemory<byte>> buffer)
          {
              _value = buffer.Value.ToArray();
          }

          return (byte[]?)_value;
      }
  }
  ```
  The buffer constructor stores its two `StrongBox`es in `_key` and `_value`; replacing a box with its result is
  Java's `keyBuffer = null`. Tried in the scratch copy: 32 B per header, and all 183 `~Header` unit tests pass on
  net10.0, `BufferConstructor_ConcurrentFirstReads_DecodeEachBufferOnce` included.

- **Fixed in 64842a100**: `RecordHeader` holds two `volatile object` fields, `_key` (its string, or its `StrongBox<ReadOnlyMemory<byte>>` until the first read decodes it) and `_value` (null, its array, or its box until the first read copies it); the first read replaces the box under `lock (this)`, and a decoded read takes no lock and allocates nothing. Measured 32 B per `new RecordHeader("k", v)` on net10.0 (48 B at `5319d8424`). `PublicRecordHeaderClassMembersTests.RecordHeader_HoldsTwoReferenceFields_SoAHeaderBuiltFromAStringHasNoBufferField` pins the two `object` instance fields by reflection, the same on every TFM (red on the four-field code).

### 131-S3-2 (Low, test-coverage): no test checks that the buffer constructor drops its buffers after the first read, which the public doc promises

- **Where**: `dotnet/src/Confluent.Kafka/Common/Header/Internals/RecordHeader.cs:53` (the doc: "the header then
  drops the buffer"), `:75`, `:93`; tests `dotnet/tests/Confluent.Kafka.UnitTests/PublicRecordHeaderClassMembersTests.cs`.
- **What**: Java's `RecordHeader#key` and `RecordHeader#value` set the buffer to null after the read, so the bytes
  behind it can be collected, and the .NET doc says the same. No test checks it: every test reads the decoded key
  and the copied value, which are the same whether the header keeps the buffer or not. Deleting the two
  assignments is caught by IDE0044 ("make field readonly", an error), but a compiling change that keeps the buffers
  passes the suite.
- **Evidence**: in a scratch copy of `5319d8424`, `_keyBuffer = new StrongBox<ReadOnlyMemory<byte>>(_keyBuffer!.Value);`
  at `:75` and `_valueBuffer = buffer;` at `:93` build with 0 errors and pass all 183 `~Header` unit tests on net10.0.
- **Fix**: add a collectability check to `PublicRecordHeaderClassMembersTests`: a
  `[MethodImpl(MethodImplOptions.NoInlining)]` helper builds the header over a key array and a value array it keeps
  nothing else of, and returns the header and a `WeakReference` to each array. After `GC.Collect()`,
  `GC.WaitForPendingFinalizers()`, `GC.Collect()`, both arrays are alive (the control); after `Key()` and `Value()`
  and the same collection, both are dead; then `GC.KeepAlive(header)`. Tried in the scratch copy: it passes 3 of 3
  at `5319d8424` and fails on the change above. It holds for the two-field shape of 131-S3-1 too.

- **Fixed in 64842a100**: `PublicRecordHeaderClassMembersTests.BufferConstructor_DropsEachBufferAtItsFirstRead` builds the header in a `NoInlining` helper over a key and a value array it keeps nothing else of, with a `WeakReference` to each: both alive after a full collection (the control), the key's dead and the value's alive after `Key()`, the value's dead after `Value()`, then `GC.KeepAlive(header)`. Red on the four-field code with your keep-the-buffers change, and on the two-field code with a static list keeping the key's box.

### 131-S5-1 (Low, docs): `dotnet/CLAUDE.md` says the null-key check runs "before the pin", which holds for the first key read only; the later read, which the same bullet says throws too, runs in the build after the key and value pins, as ffi §A4 now says

- **Where**: `dotnet/CLAUDE.md:813` (§3 call-site checks, "A null header key": "The check runs after the
  serializers and before the pin (§1), so before the core's metadata wait"). Compare
  `dotnet/.claude/rules/ffi-marshalling.md:725-730` (§A4: "checks its key at every read: before anything is
  pinned, and again in the build … On the async send the later read comes after the key and value pins") and
  `dotnet/src/Confluent.Kafka/Internal/Interop/ProducerSendMarshal.cs:106-108`.
- **What**: the same bullet (`:810-811`) says the key is checked "at every read of the key, so a key that turns
  `null` at a later read throws too". That later read is `AddHeader` → `HeaderKey` inside `BuildRecord`. On the
  sync send it runs inside `Send`'s `fixed` over the key and value (`ProducerSendMarshal.cs:48-57`). On the
  async send it runs after `pins.Pin` in `SendWithHandOver` (`:86-87`). So the check runs on both sides of
  the pin, and the sentence repeats the claim that 131-S2-2 removed from the code comment. Its conclusion still
  holds: both reads come before the core's metadata wait (ledger 607). Only "before the pin" is wrong, and it
  contradicts the new ffi §A4 bullet.
- **Evidence**: `ProducerSendMarshal.cs:106-108`: "each header key's first read are rejected before anything is
  pinned, and HeaderKey checks every later read, after the async send's pin".
  `PublicUserHeaderTests.AsyncMockSend_AUserHeaderWhoseKeyIsNullOnlyAtALaterRead_UnpinsTheKeyAndValueOnce`
  (`PublicUserHeaderTests.cs:156`) finds `PinCount == 1` for the key and the value at the rejection. That
  test exists because the later check runs after the pin.
- **Fix**: for example, "The checks run after the serializers, the first before the pin and the later ones in
  the build (§1), so all of them before the core's metadata wait, where Java's `doSend` waits for metadata
  first: …". Docs only; a fixup of `a02edf1ef`.

- **Fixed in 105116893**: the bullet now reads "The first read is checked after the serializers and before the pin (§1), the later ones in the record build, so all before the core's metadata wait, where Java's `doSend` waits for metadata first: …", matching ffi §A4; the rest of the bullet is unchanged.
