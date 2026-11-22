# Plan: consumed records wrap the native batch

Status: approved by the owner (2026-11-20). Scope: the full plan.

Milestone 23, Phase 9. Agent number N=132. Mode A (.NET only): no Rust, C ABI, header, Python or proto change.
Branch `esabellico/dotnet_async_serdes_and_zero_copy` (PR #351), on top of M23/P8 and the local merge
`b558a23f1`, in the worktree `kafka-clients-dotnet-serdes`. Never pushed by an agent. All paths below are under
`dotnet/`.

How it runs: a dotnet-actor 132 implements one step at a time and commits it with `--no-verify` after running
the step's tests (section "Gates per step"); one dotnet-critic 132 loop runs over the phase at the end. The rule
edits are drafts (section "Draft rule text"): no rule file is edited until the owner says so, and until then
this plan wins over the rule text it replaces.

## Context

Profiling PerfV3 at 2 KiB (100,000 msg/s) showed that the copy-out step costs 0.165 µs per record on the sync
and the async consumer alike. Almost none of that is copying bytes: it is about 14 P/Invokes per record, each
followed by `ThrowIfFfiFailed` (offset, timestamp, timestamp type, key, value; five calls to find out a record
has no headers; the two serialized sizes and the leader epoch). On top of that, `NextOffsets` copies a map on
every poll (0.018 µs per record).

`ConsumerRecords`, `ConsumerRecord` and `Headers` therefore wrap the native batch and read each field through
the FFI only when it is asked for.

Decisions taken with the owner:

1. **Getters are not cached.** Each scalar getter of a wrapped record is one P/Invoke on every call. The record
   object stays small and thread-safe without locks.
2. **Key and value are still deserialized inside `Poll`.** That is Java's contract:
   `RecordDeserializationException` comes from `poll()`, and the position stays on the failed record.
3. **Same branch as PR #351**, Actor 132, one Critic loop at the end.

Kept from M23/P7: release is GC-only (`ConsumerRecords` is not `IDisposable`, `GC.AddMemoryPressure` covers
the batch, ledger 537); `_recordsHeld` counts only the copy-out scope; no new FFI entry point (every getter
needed exists, including `kafka_consumer_ConsumerRecord_delivery_count_err`, which the binding did not read, so
`DeliveryCount()` was always null on a polled record).

## Design

### Batch lifetime (`SafeConsumerRecordsHandle`, `NativeConsumer`)

- Every non-empty polled batch, on the span path too, becomes a `SafeConsumerRecordsHandle` released by the GC
  only (`Retain`). A batch of 0 records is destroyed at once, and `EMPTY` or `Owned(empty, nextOffsets)` is
  returned as before.
- **Invariant:** once a `ConsumerRecord` or `ConsumerRecords` wraps a batch, no code calls `batch.Dispose()`.
  Removed: `PollTyped`'s `DestroyRecords` for a non-empty batch, `ReleaseBatch`'s "nothing handed out →
  `Dispose()`", and the lend-only dispose of `CompletePoll`. A record that references the batch is enough to
  keep it valid; each native read is followed by `GC.KeepAlive(_batch)`, so there is no `DangerousAddRef` per
  read. Pins (`BytesMemoryManager`) work as before.
- Memory pressure: the key and value bytes (`AddBytes`, as before) plus a fixed per-record constant for the
  native record and its headers object, measured from the Rust types (`ConsumerRecordInner` 152 bytes, the two
  `kafka_Bytes_t` views and the topic `CString`; documented at the constant).
- The mock's `HGlobal` key and value bytes are freed through the existing `_recordsRetained` /
  `ReleaseRetainedRecords` path, which every mock batch now takes.
- The finalizer-thread destroy is safe: it is a `Box` drop with no link to the consumer
  (`rust/src/ffi/consumer/consumer_records.rs`, `kafka_consumer_ConsumerRecords_destroy`). The record pointers
  of `records_with_partition` stay valid until the batch is destroyed; the list is destroyed after use.

### `ConsumerRecord<TKey, TValue>`

- A new internal constructor `(SafeConsumerRecordsHandle batch, IntPtr record, string topic, int partition,
  TKey? key, TValue? value, Headers? headers)`. Topic and partition stay eager (per partition; the shared topic
  string is pinned by `PublicConsumerTopicAllocationBudgetTests`). Key and value are the deserialized values.
- The public constructors keep the managed fields. Each getter tests `_batch is null`: managed returns the
  field; wrapped makes one P/Invoke (`Offset`, `Timestamp`, `TimestampType` with its pointer-to-enum cache,
  `SerializedKeySize`, `SerializedValueSize`, `LeaderEpoch` -1 → null, `DeliveryCount` -1 → null), then
  `ThrowIfFfiFailed` and `GC.KeepAlive(_batch)`.
- `Headers()`: a wrapped record creates its `Headers` on first call (`Interlocked.CompareExchange`), one
  instance per record. When a headers-form deserializer runs during `Poll`, the instance created for it is the
  one stored (the identity `SerdeHeadersTests` and `ZeroCopyDeserializationTests` pin).
- `ToString` reads through the getters, as Java's does.

### `Headers`

- New internal state: a native source (the batch and the record pointer). An untouched `Headers` makes no
  native call. The first use of any member (enumeration, `LastHeader`, `GetHeaders`, `ToArray`, `Add`,
  `Remove`, `Equals`, `GetHashCode`, `ToString`, `Entries`, the copy constructor's fast path, `SetReadOnly`)
  materializes the list once, under a lock, and from then on it behaves as before: writable, owned `Header`
  copies. The lock is also required by `RecordHeaders__as_Headers`, whose handle no other call may use at the
  same time (`rust/src/ffi/common/header/internals/record_headers.rs`). The materialization is the former
  `ConsumerRecordsMarshal.CopyHeaders`, moved.
- `NewError` materializes the headers before building `RecordDeserializationException`, so an exception the
  caller keeps does not keep the batch alive.

### `ConsumerRecords<TKey, TValue>`

- `Owned(...)` takes the batch handle. `NextOffsets()` reads `ConsumerRecords_next_offsets` on its first call
  and caches the read-only dictionary (an object, so decision 1 does not apply); copy-out no longer reads it per
  poll. On a deserialization failure `Cut` still builds an explicit managed map (cold path).
- The record lists stay managed: they hold the deserialized keys and values.

### Copy-out (`Internal/Interop/ConsumerRecordsMarshal.cs`)

- `RecordReader.Copy` and the `CopyOutAsync` loop do only `ListGet`, `ConsumerRecordKey`, `ConsumerRecordValue`,
  the deserializers and the internal constructor.
- `Rewind`, `Cut`, `LastReturned`, `FirstPositions` and `AppendFirstPositions` read the offset and leader epoch
  through the wrapped getters or `ConsumerRecordOffset`, while the batch is alive.

## Steps

- **S0** — this plan, the rule drafts, Proposed ledger rows; `design/current/STATUS.md`.
- **S1** — batch lifetime: every non-empty batch GC-released; the explicit disposes removed; the
  memory-pressure constant; every mock batch on the retention path.
- **S2** — wrapped `ConsumerRecord` getters, the internal constructor, the `_delivery_count` import, the reduced
  `RecordReader.Copy` and `CopyOutAsync` loop.
- **S3** — native-backed `Headers` with first-use materialization; `CopyHeaders` removed; `NewError`
  materializes.
- **S4** — lazy, cached `ConsumerRecords.NextOffsets()`.
- **S5** — code docs and `README.md`; the allocation budgets re-measured and lowered; the full gates.
- **S6** — PerfV3 A/B against `b558a23f1` (the Manager's).

## Verification

New unit tests (mocks, no broker; exact messages asserted where an exception is expected):

- **Getters:** wrapped getters return the native values, mock records included (sizes -1, no headers), and
  `DeliveryCount` from `_delivery_count`.
- **Lifetime:** a record read after the next `Poll`, after the consumer is disposed, and when held without its
  `ConsumerRecords`; the batch released once nothing references it (`WeakReference`, `GC.Collect` +
  `WaitForPendingFinalizers`, observed through the release); the mock's bytes freed after the last batch is
  released.
- **Headers:** an untouched `Headers` makes no native call (allocation budget with no list per record);
  concurrent first use from many threads gives one consistent list; `PublicHeadersTests` and the deserializer
  identity tests unchanged; `RecordDeserializationException.Headers()` readable after the batch is gone.
- **Failure paths:** deserialization failure and cancellation mid-batch still seek back.
- **Allocation budgets:** `PublicSyncConsumerAllocationBudgetTests` (448 B), `PublicAsyncConsumerAllocationBudgetTests`
  (272 B), `PublicConsumerMemoryAllocationBudgetTests`, `PublicConsumerTopicAllocationBudgetTests`: re-measured,
  each bound lowered to the new figure.

## Gates per step

1. `cargo build --features ffi` in `rust/` (only if the native library is missing).
2. `dotnet build Confluent.Kafka.sln -c Release`: 0 warnings, 0 errors.
3. `dotnet test` of `tests/Confluent.Kafka.UnitTests` on net8.0 and net10.0: 0 failed (net462 builds).
4. `dotnet format --verify-no-changes`.
5. Before the last commit: `make -C dotnet test-grpc-server-dotnet`.

`make verify-dotnet` (repository root) is the close gate, run by the owner.

## Out of scope

Lazy deserialization of key and value (decision 2); new FFI exports (a bulk field read, a header count);
caching scalar getters (decision 1); zero-copy header values (they stay `byte[]` copies once materialized, the
core's header values being owned `Vec<u8>`); per-field retention (one kept record keeps its whole batch).

## Ledger rows (Proposed)

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 584 | M23/P9 plan, decision 1 | Wrapped records | a polled `ConsumerRecord` wraps its native record: topic, partition, key and value are read at poll; every other getter is one P/Invoke per call, never cached. Supersedes the "owned copies" promise of the `ConsumerRecord` docs | Proposed |
| 585 | decision 2 | Deserialization | key and value are still deserialized inside `Poll`; a failure is `RecordDeserializationException` from `Poll` with the position on the failed record, as Java | Proposed |
| 586 | Design, batch lifetime | Batch release | every non-empty batch is released by the GC only, never disposed once a record wraps it; pressure = key and value bytes + a per-record constant. Extends ledger 537 to the span path | Proposed |
| 587 | Design, `Headers`; Revision (2026-11-22) | Headers | a fetched record's `Headers` is created on first use and reads the native headers once, under the batch's lock, into native-backed `Header`s whose key and value are not read yet; untouched, it makes no native call; from then on its list is managed and writable. Supersedes ledger 541 ("still copied"); revised 2026-11-22 from owned copies to the zero-copy form over the C views of PR #355 | Proposed |
| 588 | Design, `NewError` | Exception headers | `RecordDeserializationException` holds materialized headers, so it never keeps the batch | Proposed |
| 589 | Design, `ConsumerRecords` | `NextOffsets` | read on first call and cached; not copied per poll | Proposed |
| 590 | Rule drafts | Rule text W1–W7 | drafts below; applied only on the owner's explicit OK | Open |
| 596 | Revision (2026-11-22) | `Header.ValueMemory()` | new public `ReadOnlyMemory<byte>? ValueMemory()`: `null` for a null value; for a fetched header a memory over the batch (`SafeConsumerRecordsHandle.HandOut`), never copied, readable after the next poll and after the consumer is disposed; for a built header its own array. *(Deviation: Java's `Header` has only `value()`. Why: a `byte[]` cannot view the batch)* | Proposed |
| 597 | Revision (2026-11-22) | Fetched `Header` | reads its key and value through the C views on first use: `Key()` decoded once with UTF-8 (a NUL byte kept, invalid bytes as U+FFFD) and cached, `Value()` copied once and the same array returned on every call, as Java's `RecordHeader`; `ValueMemory()` never copies; a producer send of a fetched header makes no managed copy (the one copy is `kafka_common_header_Headers_add_with_key_value`'s, key into a `String` and value into a `Vec<u8>`, as the C API has no borrowed header-value form, ledger 301; the Rust producer only writes the header into the batch buffer); `RecordDeserializationException` holds owned copies | Proposed |

## Draft rule text (not applied)

Each draft replaces the quoted text. Where M23/P7's drafts R1, R4 and R5 cover the same text and are applied
first, these drafts replace those instead. Until applied, this plan wins where the two disagree.

### W1 — `CLAUDE.md` §6.4, the paragraph "Default — copy-out" (and P7's R1)

Before:

> **Default — copy-out.** For the raw-byte surface, copy each key/value into an owned managed array, then
> `ConsumerRecords_destroy`. It matches Java's owned `ConsumerRecord` shape, is safe (no `IDisposable`, no
> use-after-free, async- and thread-safe, no memory amplification), and costs one gen-0 array per raw record —
> Java's own allocation behavior. **This is the default.**

After:

> **Default — wrapped records (M23/P9, ledger 584–589).** A polled batch is a `SafeConsumerRecordsHandle`
> released by the GC only; each `ConsumerRecord` references it with its native record pointer. Topic, partition,
> key and value are read at poll, the deserializers run there (Java's `poll()` contract); every other getter is
> one P/Invoke per call, uncached, followed by `GC.KeepAlive` of the handle. `Headers` reads the native headers
> on first use, under a lock, and then holds writable owned copies. No code disposes a batch a record wraps; a
> kept record keeps its whole batch, and the pressure is the key and value bytes plus a per-record constant.
> Raw bytes through a span deserializer or `ByteArrayDeserializer` are still copied into owned arrays.

The "Deferred alternative — keep-alive (zero-copy)" paragraph: its first sentence becomes "**Zero-copy key and
value bytes** go through `BytesMemoryManager` only (P7's R1)", and the sentence "Deferred until a concrete
need." is removed.

### W2 — `CLAUDE.md` §3 consumer sketch, the `Headers()` line

Before: `public Headers Headers();    // the record's own, writable copies (not routed through a deserializer this phase)`

After: `public Headers Headers();    // the record's own; read from the batch on first use, then writable owned copies`

### W3 — `CLAUDE.md` type table, the `Headers` / `Header` row

Before: "… never on the mocks; a fetched record's are its own and writable"

After: "… never on the mocks; a fetched record's are its own, read from the batch on first use, and writable"

### W4 — `CLAUDE.md` type table, the `byte[]` key/value row; and the "Key/value type" row

Before (row): "receive: copy-out (default), keep-alive deferred — ffi §B4 / §6.4"

After: "receive: a span deserializer copies; a memory-form deserializer is zero-copy over the batch; the record
wraps the batch — ffi §B4 / §6.4"

Before ("Key/value type"): "**Consumer (receive):** wraps an owned copied array (copy-out, §6.4), not a pin."

After: "**Consumer (receive):** an owned copied array from a span deserializer, or a view over the batch from a
memory-form deserializer (§6.4), not a pin."

### W5 — `ffi-marshalling.md` §B2 table, the `ConsumerRecords_t` row

Before: "owns the fetched bytes; **copy-out default** (CLAUDE.md §6.4), keep-alive deferred;
`ConsumerRecords_destroy_err`"

After: "owns the fetched bytes and records; held by one `SafeConsumerRecordsHandle` per batch, which the
polled `ConsumerRecords`, each `ConsumerRecord` and each `BytesMemoryManager` reference; `ConsumerRecords_destroy_err`
when the GC releases the handle and no pin is open (CLAUDE.md §6.4)"

### W6 — `ffi-marshalling.md` §B2, Category 3 and Category 4 (and P7's R4)

Before (Category 3): "**Default: copy-out**: copy each element or byte into an owned managed type, then
`_destroy`. … **Keep-alive** (hold the root, expose zero-copy views, `_destroy` at `Dispose`) is a **deferred**
option for the raw-byte surface only; see CLAUDE.md §6.4."

After: "**Default: copy-out**: copy each element or byte into an owned managed type, then `_destroy`. The one
exception is the poll batch: it is **kept alive** in a `SafeConsumerRecordsHandle` that every record and memory
view of it references, and is destroyed when the GC releases the handle and no pin is open; see CLAUDE.md §6.4."

Before (Category 4): "Represent them as a transient cursor over the parent; don't let one escape the parent's
lifetime."

After: "Represent them as a transient cursor over the parent; don't let one escape the parent's lifetime. A
`ConsumerRecord_t` may be stored only beside a strong reference to its batch's `SafeConsumerRecordsHandle`
(the wrapped `ConsumerRecord`), never on its own." The "**Why:**" sentence on one `SafeHandle` per batch, never
per record, is kept.

### W7 — `ffi-marshalling.md` §B4, the paragraph "Raw bytes are the one copy-out case" (and P7's R5)

Before: the paragraph as written ("… keep-alive zero-copy is deferred (CLAUDE.md §6.4).").

After: "The poll batch outlives `Poll`: the records wrap it and read their fields through the FFI. Raw bytes are
copy-out through a span deserializer or `ByteArrayDeserializer`, and zero-copy through a memory-form
deserializer, whose `ReadOnlyMemory<byte>` is backed by a `BytesMemoryManager` that keeps the batch alive
(CLAUDE.md §6.4)."

## Revision (2026-11-22): zero-copy headers over the C views

Asked by the owner on 2026-11-22. The branch is now `esabellico/dotnet_consumer_records_and_headers_wrapped`,
stacked on `esabellico/headers_performance_rust_and_c` (PR #355), with S0–S2 cherry-picked from the parked branch.
PR #355 changes what this plan could assume: a fetched header's key and value are views of the fetch buffer in the
core, the value crosses as a `kafka_Bytes_t` view, and `kafka_common_header_Header__key_bytes` gives the key as a
`kafka_Bytes_t` view with no copy. The "zero-copy header values" exclusion of "Out of scope" is withdrawn. Still no
Rust, C ABI, header, Python or proto change on this branch: the one new P/Invoke binds an export PR #355 adds.

Native lifetime this relies on: `kafka_consumer_ConsumerRecord_headers` hands out the record's cached, `const`
headers handle, valid until the batch is destroyed; the `kafka_common_header_internals_RecordHeader_t` elements of
`kafka_common_header_Headers_to_array` are that handle's cached views, valid until its next mutation, which a
`const` handle never has; so each element, and the `kafka_common_header_Header_t` view of it, is valid as long as
the batch.

**S3 (replaces the S3 above).**

- `Headers` gets a native source, the batch handle and the record pointer. An untouched `Headers` makes no native
  call. The first use of any member materializes the list once, under a lock (`ConsumerRecord_headers`,
  `RecordHeaders__as_Headers`, `Headers_to_array`, one `RecordHeader__as_Header` per element, `List_destroy`) into
  native-backed `Header`s whose key and value are not read yet. From then on the list is managed and writable as
  before; `Add` and `Remove` change the managed list only.
- `Header` gets an internal constructor `(SafeConsumerRecordsHandle batch, IntPtr header)`. Each native read is
  followed by `ThrowIfFfiFailed` and `GC.KeepAlive(batch)`.
  - `Key()`: on first call reads `kafka_common_header_Header__key_bytes_err` (new P/Invoke returning `KafkaBytes`)
    and decodes it with `Encoding.UTF8`, caching the string (`Interlocked.CompareExchange`), as Java's
    `RecordHeader.key()` caches its decoded key. The bytes are already valid UTF-8 (the core decodes an invalid key
    as Java does), and a key holding a NUL byte is no longer cut at it (the former `Header_key` C string was).
  - `Value()`: on first call copies the value view into a `byte[]` and caches it, so every call returns the same
    array, as Java's `RecordHeader.value()` caches `Utils.toArray(valueBuffer)`.
  - New public `ReadOnlyMemory<byte>? ValueMemory()`: `null` for a null value, as `ISerializer<T>`'s
    `ReadOnlyMemory<byte>?` marks null; for a fetched header a memory over the batch through
    `SafeConsumerRecordsHandle.HandOut` (a `BytesMemoryManager`, which keeps the batch alive while reachable and
    while pinned), with no copy; for a header built with a `byte[]`, that array as memory. *(Deviation: Java's
    `Header` has only `value()`. Why: a `byte[]` cannot view the batch, and the consumer's key and value already
    have a zero-copy memory form.)*
  - `Equals`, `GetHashCode` and `ToString` keep their output and compare or print the value through its span, so
    they force no copy.
- `NewError` materializes owned copies (string keys, `byte[]` values) for `RecordDeserializationException`, so an
  exception the caller keeps does not keep the batch.
- `ProducerSendMarshal` reads each header value through `ValueMemory()`, pinned for the call, instead of `Value()`,
  so forwarding a fetched header to a producer makes no managed copy. The one copy is made by the C FFI function
  `kafka_common_header_Headers_add_with_key_value`, which builds `RecordHeader::new(key, value.to_vec())` (the key
  into a `String`, the value into a `Vec<u8>`), because the C API has no borrowed header-value form (ledger 301); the
  Rust producer makes no extra copy, writing the header into the batch buffer as wire encoding, as it does key and
  value.
- `CopyHeaders` is removed; copy-out builds the record's `Headers` with its native source.

S4–S6 as above. S5 adds tests for: a fetched header's `ValueMemory()` backed by a `BytesMemoryManager` over the
batch (`MemoryMarshal.TryGetMemoryManager`), usable after the next `Poll` and after the consumer is disposed;
`Value()` returning the same array on every call; a key with a NUL byte and a key with invalid UTF-8 (U+FFFD); a
null value (`Value()` and `ValueMemory()` null) and an empty one; concurrent first use of `Key()`, `Value()` and the
list; an untouched `Headers` making no native call; `RecordDeserializationException.Headers()` after the batch is
released; and a producer sending a fetched header.

Ledger rows (Proposed): 587 is revised to the zero-copy form above; 596, `Header.ValueMemory()` (the deviation
above); 597, a fetched `Header` reads its key and value through the C views on first use, `Key()` and `Value()`
cached as Java's `RecordHeader`, `ValueMemory()` never copied. Rule drafts W2 and W3 say "read from the batch on
first use, its values views of the batch until `Value()` copies one".
