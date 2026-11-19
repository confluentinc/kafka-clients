# Plan: serializers return `ReadOnlyMemory<byte>?`

Status: approved (ledger rows 400–406, Agreed 2026-11-06). Scope: the full plan.

Milestone 23, Phase 1. Agent number N=120. Mode A (.NET only).
Branch `prashah_dev_dotnet_serializer_readonly_memory`, cut from `origin/master` `de838bf3b` (#317), in the
worktree `.claude/worktrees/serializer-readonly-memory`. No upstream; never pushed by an agent.

---

## 0. The change in one paragraph

Today a .NET serializer must hand back a `byte[]`: a whole array, exactly the size of the data. The send path inside
the binding already works on `ReadOnlyMemory<byte>?`, which can also be a *part* of an array or memory that lives
outside .NET. Only the public method signature stops a user from passing such memory in. Emanuele Sabellico asked
(Slack, 2026-11-06) to change it now, before the preview freezes the API:

```csharp
// dotnet/src/Confluent.Kafka/Common/Serialization/ISerializer.cs
byte[]?               Serialize(string topic, T data);   // today
ReadOnlyMemory<byte>? Serialize(string topic, T data);   // after this phase
```

Every class that implements `ISerializer<T>` must change one line. Most of them keep working with no other change,
because C# turns a `byte[]` into a `ReadOnlyMemory<byte>` by itself. There is one trap (D2): a `null` `byte[]`
turns into an **empty** value, not a null one. The plan is mostly about that trap, about how long the producer reads
the memory (D3), and about the tests that prove both.

---

## 1. Words used in this plan

Each C# term once, with the nearest Rust or Python idea.

| Term | What it is | Nearest idea |
|---|---|---|
| `byte[]` | An array of bytes, an object on the .NET heap. Fixed length, and its bytes can be changed. | Rust `Box<[u8]>`; Python `bytearray` |
| class / struct | A *class* is a heap object passed by reference. A *struct* is a value, copied when passed, with no heap object of its own. | Rust `Box<T>` vs a plain `T: Copy` |
| `T?` on a struct | `Nullable<T>`: the struct plus a "has a value" flag. `HasValue` says if there is one, `.Value` reads it. `null` means "no value". | Rust `Option<T>` (`is_some()`, `unwrap()`) |
| `T?` on a class | Only a hint to the compiler that the reference may be `null`. It changes no code. | a type comment |
| implicit conversion | The compiler converts one type to another without a cast in the source. | Rust `From`/`Into`, but applied silently |
| GC | The garbage collector. It frees objects nothing refers to, and it may **move** objects to compact memory. | Rust has none; CPython frees by refcount and never moves |
| pinning | Telling the GC not to move an object for a while, so native code can keep a raw pointer to it. `fixed (...) { }` pins for one block; `memory.Pin()` returns a `MemoryHandle` that pins until you `Dispose` it. | a borrow that ends at the block's end; a guard object |
| `ReadOnlySpan<byte>` | A view: pointer + length. It can live only on the stack: it cannot be stored in a field or kept across an `await`. | Rust `&[u8]` |
| `ReadOnlyMemory<byte>` | A view that *can* be stored: a struct holding an object, an offset and a length. `.Span` gives the span. It may point into an array, at a slice of one, or at memory a `MemoryManager` owns. | Python `memoryview` |
| `MemoryManager<byte>` | A class a user writes so that a `Memory<byte>` can point at memory the user manages, for example memory from `malloc` (`Marshal.AllocHGlobal`). It implements `GetSpan()`, `Pin()` and `Unpin()`. | implementing Python's buffer protocol for your own object |
| `kafka_Bytes_t` | The C ABI's byte view: `{ const uint8_t *data; int32_t len; }`. | `&[u8]` across C |
| tombstone | A record whose value is null. On a compacted topic it deletes the key. An **empty** value is not a tombstone. | — |
| first stage | On `IAsyncProducer`, `Send` returns a `ValueTask` that completes when the Rust core has taken the record (the "hand-over"). The delivery result is a second future inside it. | — |

---

## 2. Goal

1. `ISerializer<T>.Serialize` returns `ReadOnlyMemory<byte>?`. `null` is a null key or value. An empty memory is
   empty bytes.
2. A serializer can return a slice of a larger array, a rented (pooled) array, or native memory behind a
   `MemoryManager<byte>`, and the binding sends those bytes **without a .NET-side copy**.
3. The null-vs-empty trap (D2) cannot reach a record through any serializer in this repository, and the docs warn
   users about it.
4. Tests prove the pointer the serializer hands back is the pointer the core reads, for a slice with an offset that
   is not 0, for native memory, and for null vs empty, on the blocking and the async producer.

### Why now

Changing the return type breaks every class that implements the interface. C# cannot soften this: a class must
declare exactly the interface's return type, and netstandard2.0 has no default interface methods (CS8701;
`dotnet/CLAUDE.md` "Serdes" section, ledger rows 153 and 321). So it has to land before the preview freezes the API.

---

## 3. What changes and what does not

**Changes**

- The public return type of `ISerializer<T>.Serialize`, and its doc comment (D2, D3).
- The seven built-in serdes in `Internal/Serialization/` (return type; `String` and `ByteArray` also their null
  line, D2, D6).
- `Internal/SerializedProducerRecord.cs`: it stops converting `byte[]?` to `ReadOnlyMemory<byte>?`, because the
  serializer already returns that.
- Every serializer and every caller of `Serialize` in the repository: tests and the soak client (§6).
- Rules: `dotnet/CLAUDE.md` §3 and §4, `ffi-marshalling.md` §A4 (drafts R1–R4, §8).

**Does not change**

- The wire format, the Rust core, the C ABI, the generated header, P/Invoke declarations (no new import), Python,
  the gRPC protos.
- The send path's pinning code (`SendPins`, `ProducerSendMarshal`): it already takes `ReadOnlyMemory<byte>?`.
- `IDeserializer<T>` and the consumer (the deserializer idea is "later", §13).
- `Header.Value` (stays `byte[]?`, D5).
- What the built-in serdes produce, byte for byte.

### How the bytes travel, before and after

```
today:  value --Serialize--> byte[]? --(SerializedProducerRecord converts)--> ReadOnlyMemory<byte>?
                                                     --pin--> kafka_Bytes_t {ptr,len} --> core copies into its batch
after:  value --Serialize--> ReadOnlyMemory<byte>? ------------------------------------> (same from here on)
```

Facts behind this picture (verified before this plan; not re-derived):

- `Internal/SerializedProducerRecord.cs` stores `Key` / `Value` as `ReadOnlyMemory<byte>?` (constructor at L29) and
  converts the serializer's `byte[]` with an explicit ternary at L66-79.
- The async send pins with `memory.Pin()` into a `MemoryHandle` (`Internal/Interop/SendPins.cs` `PinOne`, L68-77) and
  releases at the hand-over (`Internal/AsyncDeliveryState.cs` L119 and L137, both **before** the first stage
  completes).
- The blocking send uses `.Span` and `fixed` (`Internal/Interop/ProducerSendMarshal.cs` `Send`, L34-70).
- The Rust core borrows the bytes and copies them once, into its batch: `BytesPassthrough`
  (`rust/src/ffi/producer/kafka_producer.rs:100-122`, "borrowed rather than copied: the producer copies them once,
  into the batch"); the serialized slices go to the accumulator as `as_deref()` borrows
  (`rust/src/producer/kafka_producer.rs:2175-2245`).
- A scratchpad run (net8.0) confirmed: `return byteArray;` still compiles; a slice `new ReadOnlyMemory<byte>(big, 0,
  5)` keeps the same array; `MemoryManager<byte>` over `Marshal.AllocHGlobal` memory: `.Memory.Pin()` gives the same
  native pointer. This plan's own run added: `fixed` over `.Span` of such memory, and `Pin()` of an array slice at
  offset 4, both give `base + offset`.

---

## 4. Decisions

Each decision puts Python first, then Java, then .NET. Python decides ties. "Row" is the ledger row in
`DOTNET-PORT-DECISIONS.md` (repository root, untracked). All rows are Agreed (2026-11-06).

### D1. The exact type (row 400)

| | Python | Java | .NET today |
|---|---|---|---|
| What a serializer returns | typed `bytes \| None` (`common/serialization/serializer.py:47-48`), but `serialize()` accepts `bytes`, `bytearray` and `memoryview` (`producer/_base.py:237-248`) | `byte[] serialize(String topic, T data)` (`Serializer.java:64`) | `byte[]? Serialize(string topic, T data)` (`ISerializer.cs:28`) |
| Null | `None` = null key or value | `null` | `null` |
| Copy before the core | `bytes`: none, the C record points into it (`_confluentkafka.c:466-469`). `bytearray` / `memoryview`: **copied** to `bytes` (`_base.py:245-246`), since the C record takes only `bytes` (`_confluentkafka.c:430-438`) | none; `send` copies into the batch | none (the array is pinned) |

Options for .NET:

| Option | `return array;` still compiles | null vs empty | slices and native memory | new public type | Verdict |
|---|---|---|---|---|---|
| (a) `ReadOnlyMemory<byte>?` | yes | **trap** (D2) | yes | no | **pick** |
| (b) `ReadOnlyMemory<byte>` (no `?`) | yes | cannot say null: `default` and empty are the same value | yes | no | loses the tombstone; reject |
| (c) `IMemoryOwner<byte>?` | no (needs a wrapper object) | clear | yes | no | one extra object per record, and the producer would have to dispose it at the hand-over; not asked; reject |
| (d) a custom struct with conversions from `byte[]?` and `ReadOnlyMemory<byte>` | yes | no trap (scratchpad: `return null`, a null `byte[]` variable and `cond ? null : array` all give null) | yes | **yes**, which neither Java nor Python has (DoD #7) | adding a third conversion, from `ReadOnlyMemory<byte>?`, makes `return null;` fail to compile (CS0037, scratchpad), so a value already typed `ReadOnlyMemory<byte>?` needs `.Value`; reject (row 400) |
| (e) keep `byte[]?` and add a second interface the producer detects at run time | yes | clear | yes | yes (a second interface) | not breaking, but two serializer shapes for one job, and Emanuele asked for the change now; reject |

**Pick: (a) `ReadOnlyMemory<byte>?`.** It is what Emanuele asked for and what the send path already carries. It
follows Python's *shape* (a serializer may return any buffer) and does better than Python on copies: .NET sends a
slice or native memory in place, where Python copies a `bytearray` or `memoryview`. It deviates from Java's `byte[]`;
record the deviation in `dotnet/CLAUDE.md` §3 (R2).

### D2. Null vs empty: the trap (row 401)

Python and Java have no trap: `None` / `null` is null, `b""` / `new byte[0]` is empty, and nothing converts one into
the other. C# has one, because of the implicit conversion from `byte[]` to `ReadOnlyMemory<byte>`:

```csharp
// All three compile with 0 warnings (Nullable enabled), and all three turn null into an EMPTY value:
ReadOnlyMemory<byte>? A(byte[]? data) => data;                                   // HasValue == true, Length 0
ReadOnlyMemory<byte>? B(string? s)    => s is null ? null : Encoding.UTF8.GetBytes(s);   // same
ReadOnlyMemory<byte>? C(byte[]? data) => data is null ? null : data;             // same

// These give a real null (HasValue == false):
ReadOnlyMemory<byte>? D(byte[]? data) { if (data is null) { return null; } return data; }
ReadOnlyMemory<byte>? E(byte[]? data) => data is null ? (ReadOnlyMemory<byte>?)null : data;
ReadOnlyMemory<byte>? F(byte[]? data) => data?.AsMemory();
ReadOnlyMemory<byte>? G()             => default;   // null too; but default(ReadOnlyMemory<byte>) is EMPTY
```

Why `B` and `C` fail: the type of `cond ? null : array` is `byte[]`, so the `null` is a null *array*, which then
converts to an empty memory. Results from this plan's scratchpad runs (net8.0): A, B and C `HasValue == true`; D, E,
F and G `HasValue == false`.

The producer cannot catch it at run time: a null array converts to `default(ReadOnlyMemory<byte>)`, which is the very
same value as `ReadOnlyMemory<byte>.Empty`. And it matters: a user's tombstone would silently become an empty value,
so a compacted topic would keep the key.

Three serializers in this repository hit it if only the return type is changed: `StringSerde` (form B),
`ByteArraySerde` (form A) and the test serializer `CallLog` (`PublicProducerSendHeadersTests.cs:228`, form B).
M22/P2 adds two more (§11). The soak serializer already uses form D, and the other test serializers delegate, throw
or already produce a `ReadOnlyMemory<byte>?` (§6).

**Pick: guard with docs and tests; no analyzer.**

1. The `Serialize` doc says it in one sentence (D3's text).
2. In this repository a serializer returns `null` in its own `return` (form D) or as `(ReadOnlyMemory<byte>?)null`
   (form E), never through `cond ? null : array` or a bare `byte[]?` (R2, R4).
3. Every built-in gets a null-in → null-out test and an empty-in → empty-out test; the send path gets null-vs-empty
   tests for the key and the value (§7).
4. The breaking-change note shows the trap with a wrong and a right version (§12).

**A CKRDA analyzer: no.** It would be cheap to write (flag the `byte[]` → `ReadOnlyMemory<byte>` conversion into a
`Nullable` when the array may be null), but the CKRDA analyzers check only this repository: `Directory.Build.props`
L21-26 adds them as `PrivateAssets="all"`, so they never reach users. Inside the repository the three sites (five after M22/P2) are
few, and the tests above catch them. An analyzer shipped to users would be a new package; not for this phase.

### D3. How long the producer reads the memory (row 402)

`ReadOnlyMemory<byte>` is a view. It does not own the bytes. So the doc must say when the owner may change or free
them.

| | Python | Java | .NET (both today and after) |
|---|---|---|---|
| When the bytes may change | `bytes` cannot change. A `bytearray` / `memoryview` is copied when the serializer returns, so it may be reused at once | `send` copies the `byte[]` into the batch before it returns | blocking `Send`: read until it returns or throws. Async `Send`: until it throws or its `ValueTask` has completed, in any state (ledger 73; `ffi-marshalling.md` §A4, L663-670) |
| Mocks | pure Python, keep the objects | `MockProducer` keeps the record objects | same as the real producers: `MockProducer` / `AsyncMockProducer` send through the same native path (`MockProducer.cs:100-103`, `AsyncMockProducer.cs:128-131`). The native mock is built without serializers (`NativeProducer.cs:122`, `kafka_producer_MockProducer_with_auto_complete`) and never reads the bytes (`rust/src/ffi/producer/mock_producer.rs:23-26`); .NET reads only the history's **count** (`NativeProducer.cs:378-393`) |

Two .NET facts the Critic should know (no doc text needed for them):

- The async path calls the memory's `Pin()` once and `Unpin()` once (through `MemoryHandle.Dispose`) at the
  hand-over, so a user's `MemoryManager` sees a balanced pair before the `ValueTask` completes.
- The blocking path never calls `Pin()`: it uses `fixed` over `.Span`, which pins managed memory and is a no-op for
  native memory. That is correct: the caller's thread is inside `Send` the whole time.

The serializer itself is never told when the send is done (Java's is not either). So a serializer that returns a
buffer it will reuse is safe only when the caller does not overlap sends on that buffer. The doc says what the
producer does; it does not promise a release hook.

**Pick: this doc text on `ISerializer<T>.Serialize`** (the type's `<summary>` is left alone; see §11):

```csharp
/// <summary>Encodes <paramref name="data"/>, which may be <see langword="null"/>.</summary>
/// <param name="topic">The topic the record is sent to.</param>
/// <param name="data">The value to encode.</param>
/// <returns>
/// The bytes, or <see langword="null"/>, which is sent as a null key or value. An empty memory is sent as empty bytes.
/// A <see langword="null"/> <c>byte[]</c> converts to an empty memory, not to <see langword="null"/>: return
/// <see langword="null"/> itself.
/// </returns>
/// <remarks>
/// The producer reads the bytes where they are and copies them only into its record batch. It reads them until
/// <c>Send</c> returns or throws, or, on an async producer, until <c>Send</c> throws or the <c>ValueTask</c> it returns
/// has completed. Keep them unchanged until then. The key and the value are read after both serializers ran, so they
/// must not share a buffer that the second call overwrites.
/// </remarks>
ReadOnlyMemory<byte>? Serialize(string topic, T data);
```

`IAsyncProducer.Send`'s existing sentence ("Don't change or reuse the key and value buffers until `Send` throws or the
`ValueTask` it returns has completed", `IAsyncProducer.cs` L95-96, L126, L166) stays as it is: it is ledger 73's text
and already true for memory.

### D4–D10. The smaller decisions (row 403, one ruling for the table)

| # | Question | Python | Java | .NET pick |
|---|---|---|---|---|
| D4 | Where copies still happen; what "zero copy" means | `bytes`: no copy before the core; `bytearray` / `memoryview`: one copy in `serialize()`; then the core copies into its batch | the `send` call copies into the batch (`RecordAccumulator.append`); `ByteBufferSerializer` also copies a slice or a direct buffer into a new `byte[]` (it returns the array as it is only for a whole array at offset 0) | **"No .NET-side copy."** The memory is pinned or `fixed`, passed as a `kafka_Bytes_t`, borrowed by the C ABI (`producer_record.rs:719-759` keeps the `void *`; `BytesPassthrough` borrows), and copied **once**, by the core, into its batch, as Java and Python do. The saving is the copy a user's serializer had to make to produce an exact-size `byte[]` from a slice, a pooled array or native memory. Never write "zero copy" without "on the .NET side". |
| D5 | Header values | any buffer, borrowed by the C extension (`_confluentkafka.c:332-340`), then **copied** by the C ABI ("the C API has no borrowed form for a header value") | `byte[] value()` | **Out of scope.** `Header.Value` stays `byte[]?` (`Header.cs:35,45`). The send copies header values through the same C call (`ProducerSendMarshal.cs` L244 comment), so memory would save nothing; `Header` is also the consumer's type and has value equality. |
| D6 | Built-in serdes | the factories return `bytes` | `byte[]` | **Return type only; stay simple and GC-owned.** `Int32`, `Int64`, `Double`, `Guid` and `String` keep allocating a fresh array (`new[] { … }`, `GetBytes`), which the GC frees when nothing holds it; no pooling, because a serializer is not told when the send is done (D3). `ByteArray` returns the caller's own array, no copy, with the null form E. `Null` keeps `=> null`. `ISerde<T>` and `Serdes` need no change. |
| D7 | A built-in `ReadOnlyMemory<byte>` serde | none; `bytes_serializer()` passes a `memoryview` through and `serialize()` copies it | `Serdes.ByteBuffer()` (`ByteBufferSerializer`, which copies slices) | **Not now.** A user writes it in one line (§12). Adding it later is not breaking. |
| D8 | Perf A/B | — | — | **No A/B.** The `byte[]` path runs the same code: the array-to-memory conversion moves from `SerializedProducerRecord` into the serializer's `return`, and a 24-byte struct is returned by value. The allocation-budget tests (`PublicProducerSendAllocationBudgetTests`, `PublicSyncProducerSendAllocationBudgetTests`) guard per-send allocations and run in the unit gate. |
| D9 | Mode | — | — | **Mode A.** No Rust, no C ABI, no header, no new P/Invoke, no Python, no proto change. The C ABI already takes any pointer + length. |
| D10 | netstandard2.0 | — | — | **Available.** `System.Memory` 4.5.5 (`Confluent.Kafka.csproj` L146-147) gives `ReadOnlyMemory<T>`, `MemoryManager<T>`, `MemoryHandle` and `MemoryMarshal`; `SendPins` already calls `Pin()` on all three targets. |

### D11. The serializer's headers form, in the same break? (row 404)

Java has `default byte[] serialize(String topic, Headers headers, T data)` (`Serializer.java:84`). Python always
passes the headers. .NET has no headers form (ledger 153; a member added to the interface breaks implementers,
row 334). Since this phase breaks every implementer anyway, one might add the headers parameter now, so users change
their serializers once.

**Pick: no, not in this phase.** The headers form needs its own design: what the producer passes when a record has no
headers (a new `Headers` per record is a hot-path allocation), and when the headers turn read-only. It can also come
later **without** a break: an optional second interface the producer detects with a type test, the same way Emanuele
proposes for the deserializer (§13). The shape stays open under row 153.

### D12. The rule drafts (row 405)

Approve R1–R4 (§8) word for word, as drafted for the picks above. A different ruling on rows 400–404 is redrafted
before S0.

### D13. Logistics (row 406)

- N=120. Personas `dotnet-actor` and `dotnet-critic` (copied to the root `.claude/agents/`, untracked, per
  `dotnet/CLAUDE.md` §8.4). Comments in `dotnet/COMMENTS.120.md`; resolved to `dotnet/COMMENTS.DONE.120.md`; neither
  is committed at the binding root. A Critic pass after each Actor commit.
- Plan folder `dotnet/design/history/M23/P1-serializer-readonly-memory/` (this file). §8.4 puts plans at
  `design/history/<Milestone>/<Phase>/`, and the folder names are `P<n>-<slug>`. M22 already has two `P1-*` and two
  `P2-*` folders from branches made in parallel; a new milestone avoids a third clash. (The alternative is
  `M22/P4-serializer-readonly-memory`.)
- M22/P2 (PR #320, per ledger 399) lands first; then this branch rebases onto master and fixes M22/P2's five test
  serializers (§11). If this branch lands first, M22/P2's rebase makes the same fixes.
- Squash or not: decided at the end, as in M22.

---

## 5. Decision summary

| D | Pick | Row |
|---|---|---|
| D1 | `ReadOnlyMemory<byte>?` | 400 |
| D2 | docs + explicit null forms + tests; no analyzer | 401 |
| D3 | the doc text above; no release hook | 402 |
| D4–D10 | as the table | 403 |
| D11 | headers form not in this break | 404 |
| D12 | R1–R4 verbatim | 405 |
| D13 | N=120, M23/P1, M22/P2 first | 406 |

---

## 6. Every place to update

Bounded grep over `dotnet/` at `de838bf3b` (bin and obj excluded).

| Where | Files | What changes |
|---|---|---|
| Public interface | `Common/Serialization/ISerializer.cs` (1) | return type and method doc (D3) |
| Built-ins | `Internal/Serialization/` `ByteArraySerde`, `StringSerde`, `Int32Serde`, `Int64Serde`, `DoubleSerde`, `GuidSerde`, `NullSerde` (7) | return type; `ByteArraySerde` and `StringSerde` also form E for null |
| Internal | `Internal/SerializedProducerRecord.cs` (1) | `keyBytes` / `valueBytes` and `SerializeField` become `ReadOnlyMemory<byte>?`; the L76-77 ternaries go; no `ToArray()` anywhere |
| Unit tests: serializers | `TestSerializers.cs` (3: `RecordingSerializer`, `ThrowingSerializer`, `NullableInt64Serializer`), `PublicProducerSerializerFailureTests.cs:533`, `PublicProducerSendHeadersTests.cs:228` (5 in 3 files) | return type; `CallLog` (`PublicProducerSendHeadersTests.cs:228`) also form E; the others delegate, throw, or (after the change) already return `ReadOnlyMemory<byte>?` |
| Unit tests: callers that read the result | `SerdesTests.cs` (~24 lines), `PublicProducerTypedSendTests.cs` (6), `PublicConsumerTypedPollTests.cs` (4), `PublicConsumerTfmSmokeTests.cs` (1) (~35 lines in 4 files) | `byte[]? x = s.Serialize(…)` → `s.Serialize(…)?.ToArray()`; `Assert.Equal(bytes, s.Serialize(…))` compares contents (see the note below); `Assert.Same` at `PublicProducerTypedSendTests.cs:239` → a same-array check (U3) |
| Soak | `soak/SoakClient/SoakRecord.cs:95` (1 serializer), `soak/SoakClient.Tests/SoakRecordTests.cs` (6 lines) | return type only (its `if (data is null) return null;` is already form D); tests read `?.ToArray()` |
| gRPC server and its tests | 0 | they use `Serdes.ByteArray` / `Serdes.String` only |
| IntegrationTests | 0 to change | new tests in S3 |
| PerfV2, PerfV3, PerformanceTests, PackageSmoke | 0 | `Serdes.ByteArray` only |
| Analyzers and their tests | 0 | — |
| `dotnet/README.md`, `soak/README.md` | 0 | no serializer code in them |
| Rules | `dotnet/CLAUDE.md` (3 places), `ffi-marshalling.md` (1) | R1–R4 |
| Status | `dotnet/design/current/STATUS.md` (1 new top entry) | S4 |
| After M22/P2 lands | `tests/.../Internal/SerdeDisposalTests.cs` (3, `=> null`), `PublicSerializerDisposalTests.cs` (2, `=> data`) | §11 |

**Note for the test callers.** `Assert.Equal(new byte[] { 1, 2 }, s.Serialize(…))` may still **compile** after the
change: a generic `Equal<T>(T, T)` infers `T` = `ReadOnlyMemory<byte>?` and compares the two memories by reference
(same object, offset and length), not by contents. A scratchpad mimic of `Equal<T>(T, T)` returned `false` for equal
contents; xUnit 2.9.3's own overload choice was not checked. Either way, each such assert must become a content
compare (for example `Assert.Equal(expected, s.Serialize(…)?.ToArray())`). A small test helper is fine. Lines that
assign to `byte[]?` do not compile, so the compiler finds those.

---

## 7. Tests that prove it

**Unit tests** (no broker; `MockProducer` / `AsyncMockProducer` and internal tests; net8.0 and net10.0):

- **U1 shape.** By reflection, `ISerializer<>.Serialize` returns `Nullable<ReadOnlyMemory<byte>>`.
- **U2 built-ins, null and empty.** For `String`, `ByteArray` and `Null`: `null` in → `HasValue == false`. For
  `String("")` and `ByteArray(Array.Empty<byte>())`: `HasValue == true`, `Length == 0`. (Today's `Assert.Null` lines
  for null already catch the trap; the empty cases are new.)
- **U3 `ByteArray` does not copy.** `MemoryMarshal.TryGetArray` on the result gives the caller's array, offset 0,
  full length.
- **U4 the record keeps what the serializer returned.** `SerializedProducerRecord.Serialize` with serializers that
  return null, empty, a slice at offset 3, and native memory: `Key` / `Value` have the same `HasValue`, `Length` and
  backing (array and offset, or native pointer).
- **U5 the pointer reaches the C record (async path).** `SendPins.Pin` with (a) native memory from a
  `MemoryManager<byte>` over `Marshal.AllocHGlobal`, sliced at offset 3, and (b) an array slice at offset 4: the
  `kafka_Bytes_t`'s `data == base + offset` and `len == length`; then `ProducerSendMarshal.BuildRecord` and read the
  record's key and value pointers back, as `Interop/ProducerRecordBuildTests` already does. Extends
  `Interop/ProducerSendPinLifetimeTests`.
- **U6 the borrow ends when the doc says.** A counting `MemoryManager<byte>`: after `AsyncMockProducer.Send`'s
  `ValueTask` completes, `Pin` and `Unpin` were each called once for the key and once for the value; on a send that
  fails before the hand-over (a topic with a NUL, rejected by `RejectAlteredStrings`), also balanced (zero or one
  each). After `MockProducer.Send` returns, no pin is outstanding.
- **U7 allocations.** The existing allocation-budget tests stay green unchanged (the `byte[]` path). Optional: a
  native-memory value on the sync and async send adds no value-sized managed allocation.

**Broker tests** (`tests/Confluent.Kafka.IntegrationTests/KafkaProducerBrokerTests.cs`, Docker), with a test
pass-through `ISerializer<ReadOnlyMemory<byte>?>`; read back with the file's existing consumer helper:

- **B1 blocking `KafkaProducer`.** Key: a 4-byte slice at offset 3 of a 16-byte array. Value: 1 KiB of native memory
  behind a `MemoryManager<byte>`. The consumed key and value are exactly those bytes; `RecordMetadata` reports sizes 4
  and 1024.
- **B2 async `AsyncKafkaProducer`.** The same, and right after the `ValueTask` completes (before the delivery `Get`),
  overwrite and free the native memory. The consumed value is still the original bytes: the core no longer reads the
  buffer after the first stage.
- **B3 null vs empty, both producers.** (key empty, value null) → consumed key empty, not null; value null
  (tombstone); sizes 0 and -1. (key null, value empty) → the reverse.

The offset tests (U4, U5, B1) are there because an offset bug is the likely regression: today's `fixed` over `.Span`
and `Pin()` both include the offset, but any new code that reaches the array (`MemoryMarshal.TryGetArray`) must add
`segment.Offset` itself.

---

## 8. Rule drafts R1–R4 (for row 405)

**R1.** `dotnet/CLAUDE.md` §3, the `ISerializer<T>` sketch (L611 at `de838bf3b`), becomes:

```csharp
    ReadOnlyMemory<byte>? Serialize(string topic, T data); // Java Serializer<T>.serialize returns byte[]; see below
```

**R2.** `dotnet/CLAUDE.md` §3, the deviation bullet "**`Serialize` returns `byte[]?`** …" (L642-645 at `de838bf3b`) is replaced by:

> - **`Serialize` returns `ReadOnlyMemory<byte>?`**, not Java's `byte[]` (M23/P1, ledger 400). Python's serializer may
>   return any buffer; .NET's form of that is `ReadOnlyMemory<byte>`, which can be a whole array, a slice of one, or
>   native memory behind a `MemoryManager<byte>`, and the producer sends it with no .NET-side copy (ffi §A4). `null`
>   is a null key or value (a tombstone for the value); an empty memory is empty bytes. A `byte[]` converts by
>   itself, but a null `byte[]`, and `cond ? null : array`, convert to an **empty** memory (ledger 401): the binding's
>   serializers return `null` in its own `return` or as `(ReadOnlyMemory<byte>?)null`. The producer reads the memory
>   until `Send` returns or throws, or, on the async producers, until the `ValueTask` it returns has completed (ledger
>   73, 402).

**R3.** `dotnet/CLAUDE.md` §4, the **Serializers** row (L765): `` `ISerializer<T>` (`byte[]? Serialize(topic, T)`) ``
becomes `` `ISerializer<T>` (`ReadOnlyMemory<byte>? Serialize(topic, T)`, M23/P1) ``; the rest of the row is unchanged.

**R4.** `ffi-marshalling.md` §A4, at the end of the "Sentinels" bullet (L671-681 at `de838bf3b`, before its **Note:**), add:

> The key and value reach `SerializedProducerRecord` exactly as the serializers return them
> (`ReadOnlyMemory<byte>?`): absent is `HasValue == false`. No code converts a `byte[]?` to `ReadOnlyMemory<byte>?`
> by the implicit conversion, which turns a null array into an empty value (ledger 401), and no code copies the
> memory (`ToArray()`, a new array) on the way to the pin.

---

## 9. Steps and commits for the Actor

Commit after each step, with a clear message. A Critic pass after each commit. `dotnet/COMMENTS.120.md` is checked
before every step; fixes are `fixup!` commits naming the original.

- **S0 rules.** R1–R4 as approved, nothing else. `docs(dotnet): serializers return ReadOnlyMemory<byte>? (rules)`.
- **S1 the change.** One commit, because the interface change breaks every implementer and the tree must compile:
  `ISerializer.cs` (signature and D3's doc), the seven built-ins (D6; forms D/E for `String` and `ByteArray`),
  `SerializedProducerRecord.cs`, and every repository serializer and caller in §6 (tests, soak). No behaviour
  change; every existing test stays green. Subject `feat(dotnet)!: ISerializer<T>.Serialize returns
  ReadOnlyMemory<byte>?`, with the breaking-change note (§12) in the body.
- **S2 unit tests.** U1–U6 (U7 optional).
- **S3 broker tests.** B1–B3.
- **S4 close-out.** If M22/P2 has merged: rebase onto master and make the §11 fixes. If not: preview the merge with
  `git merge-tree --write-tree prashah_dev_dotnet_config_constants HEAD` (it reads the branch; it changes no tree or
  ref) and record the result in `COMMENTS.DONE.120.md`. Add the `STATUS.md` top entry (one entry, the evidence once;
  no progress log in this PLAN). Run the final gates (§10).

Working rules for the Actor: `git -C` and absolute paths; no bare `cd`; never touch the main tree, which is on
Pranav's `prashah_dev_dotnet_config_constants`. Shell traps here: zsh aborts a command when a glob matches nothing
(use `find`, quote `--include` patterns); `grep` may be ugrep and `cat` may be bat; a test filter that matches nothing
"passes"; check the test count.

---

## 10. Definition of done (`dotnet/CLAUDE.md` §7.5)

**Every commit:**

- Release build, 0 warnings and 0 errors (the CKRDA analyzers run in it): the solution, the gRPC server and its
  tests, soak and its tests, PerfV3, PerfV2, PerformanceTests. The library builds for netstandard2.0, net8.0 and
  net10.0.
- `dotnet format --verify-no-changes` clean on the same targets.
- Unit tests on net8.0 and net10.0; the test count goes up by the new tests and nothing is skipped that was not.
- `dotnet/`-only commits may use `--no-verify` after these gates (no Rust or C changed).

**Once, at S4,** on a quiet machine (no other `cargo test` / `dotnet test`, `docker ps` empty):

- `RUST_TEST_THREADS=6 make verify-dotnet-macos-docker`.
- `make test-integration-dotnet-native`, and the IntegrationTests project against Docker (on macOS
  `make test-integration-dotnet` only prints a SKIP banner and exits 0).
- On the PR, CI runs `make verify-dotnet` on Linux (with the perf stage) and the SSL / SASL_SSL arms.

**Also:** DoD #7, no new public type (the test pass-through serializer and the counting `MemoryManager` are test-only);
DoD #8, no TODO; DoD #10, no new per-send allocation (U7).

---

## 11. Merging with M22/P2

M22/P2 is the unmerged branch `prashah_dev_dotnet_config_constants` (master `96b786709` + 3 commits, merged with
`de838bf3b`). Read with `git -C <main tree> show prashah_dev_dotnet_config_constants:<path>`.

**Text conflicts to expect:**

1. `dotnet/CLAUDE.md` §4 **Serializers** row. Both branches edit this line. Keep M22/P2's row and swap
   `byte[]? Serialize(topic, T)` for `ReadOnlyMemory<byte>? Serialize(topic, T)` (R3).
2. `dotnet/design/current/STATUS.md`: both add a top entry. Keep both.
3. Probably clean, but check: `dotnet/CLAUDE.md` §3, where M22/P2 rewrites the "Headers overload deferred" bullet and
   the `ISerde<T>` bullet, with two unchanged lines above and three below the bullet R2 replaces; and `ISerializer.cs`,
   where M22/P2 changes the type's `<summary>` (the `IDisposable` sentence) and this phase changes the method's
   `<returns>`, `<remarks>` and signature, with seven unchanged lines between. That is why D3 puts its text on the method, not on the type.

**Compile breaks after the merge** (no text conflict, but the build fails): M22/P2's new test serializers.

- `tests/Confluent.Kafka.UnitTests/Internal/SerdeDisposalTests.cs`: `CountingSerde`, `ThrowingSerde`,
  `AsyncOnlySerde` (L175, L203, L289 on that branch): `=> null`, so the return type only.
- `tests/Confluent.Kafka.UnitTests/PublicSerializerDisposalTests.cs`: `DisposableSerializer`, `AsyncOnlySerializer`
  (L644, L674): `=> data` is form A, so the return type **and** form E.

**No overlap:** M22/P2's `NativeProducer.cs`, `KafkaProducer.cs`, `AsyncKafkaProducer.cs`, `ISerde.cs`,
`IDeserializer.cs`, `SerdeDisposal.cs` and `PublicNamespaceLayoutTests.cs` changes are about disposal and config
constants; this phase does not touch those files (it changes `SerializedProducerRecord.cs`, which M22/P2 does not).

---

## 12. The breaking-change note for users

To go in S1's commit body, the PR description and the `STATUS.md` entry.

> **Breaking: `ISerializer<T>.Serialize` returns `ReadOnlyMemory<byte>?` instead of `byte[]?`.** A serializer can now
> return part of an array or native memory, and the producer sends it without copying it on the .NET side. Change
> the return type of your `Serialize` method. Returning a `byte[]` still works as it is.
>
> ```csharp
> // Before
> public sealed class OrderSerializer : ISerializer<Order>
> {
>     public byte[]? Serialize(string topic, Order data)
>     {
>         if (data is null) { return null; }
>         return JsonSerializer.SerializeToUtf8Bytes(data);
>     }
> }
>
> // After: only the return type changes
> public sealed class OrderSerializer : ISerializer<Order>
> {
>     public ReadOnlyMemory<byte>? Serialize(string topic, Order data)
>     {
>         if (data is null) { return null; }                // a null value: a tombstone
>         return JsonSerializer.SerializeToUtf8Bytes(data); // a byte[] converts by itself, no copy
>     }
> }
> ```
>
> **Watch out for null.** A `null` `byte[]` converts to an **empty** value, not to `null`. This compiles with no
> warning and turns your tombstones into empty values:
>
> ```csharp
> // WRONG: a null string becomes an empty value
> public ReadOnlyMemory<byte>? Serialize(string topic, string data) =>
>     data is null ? null : Encoding.UTF8.GetBytes(data);
>
> // RIGHT
> public ReadOnlyMemory<byte>? Serialize(string topic, string data) =>
>     data is null ? (ReadOnlyMemory<byte>?)null : Encoding.UTF8.GetBytes(data);
> ```
>
> **New: send memory you already have, with no copy.**
>
> ```csharp
> public sealed class MemorySerializer : ISerializer<ReadOnlyMemory<byte>?>
> {
>     public ReadOnlyMemory<byte>? Serialize(string topic, ReadOnlyMemory<byte>? data) => data;
> }
> ```
>
> Keep the bytes unchanged until `Send` returns, or, on `AsyncKafkaProducer`, until the `ValueTask` that `Send`
> returns has completed. Code that called `Serialize` itself and kept a `byte[]` can use `serializer.Serialize(…)?.ToArray()`.

---

## 13. Later: the deserializer (out of scope)

Emanuele's second idea: an optional deserializer overload that takes `ReadOnlyMemory<byte>`, which the consumer
detects at run time (a type test, so not breaking) and which takes ownership of the record's bytes through a
refcount (`kafka_consumer_ConsumerRecord__take_key` / `__take_value`). Those functions are not in the C ABI at
`de838bf3b` (no `take_key` in `rust/src`), so it is Mode B: the Rust core first. It is the "concrete need" that
`dotnet/CLAUDE.md` §6.4 defers: .NET lacked Python's refcounted `memoryview`, and a refcount the user releases gives
it. It can land after the preview. The serializer's headers form (D11) may come the same way.

---

## 14. What the Critic should look at

- **Null vs empty everywhere.** Every `return` of a `ReadOnlyMemory<byte>?` in `src/`, `tests/`, `soak/`: no form A,
  B or C (D2). Grep `? null :` and `=> data;` in serializers.
- **No new copy.** No `ToArray()`, `new byte[…]` or `CopyTo` between the serializer's return and the pin
  (`SerializedProducerRecord`, `ProducerSendMarshal`, `SendPins`).
- **Offsets.** Any use of `MemoryMarshal.TryGetArray` adds `Offset`; U4, U5 and B1 cover a non-zero offset.
- **Pins.** `Pin()` / `Dispose` balanced on every path of the async send, including failures (U6); the sync path stays
  `fixed` (ffi §A4).
- **Docs match the code.** D3's text against `AsyncDeliveryState` (pins released before the first stage completes)
  and against ledger 73.
- **Tests prove the claim.** U5 checks pointer identity, not only that a send succeeds; B2 frees the memory after the
  first stage; asserts compare contents, not `ReadOnlyMemory` references (§6 note).
- **Surface.** The return type is the only public change; no new public type; the built-ins' bytes are unchanged.
- **Shape, not logic** (`dotnet/CLAUDE.md` §8.2), and the comment rule (§4 Comments, ledger 38).

---

## 15. Questions for Pranav

All ruled: ledger rows 400–406, Agreed 2026-11-06.
