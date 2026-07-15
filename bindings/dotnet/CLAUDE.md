# Confluent Kafka .NET Binding — Claude Rules

**What this file is.** The concrete rulebook for the .NET binding under
`bindings/dotnet/`. It is the .NET-specific instance of the shared,
language-agnostic binding rulebook `bindings/CLAUDE.md` (the 4-layer mental
model + cross-binding conventions), itself governed by the repo-root `CLAUDE.md`
(Rust-core translation rules). This file holds the *concrete* realization — real
files, real `*_t` handles, the real P/Invoke call path — never a re-statement of
the generic model or of the boundary deep-dive.

**How it loads.** Working anywhere under `bindings/dotnet/` stacks three
rulebooks: root `CLAUDE.md` → `bindings/CLAUDE.md` → this file. Heavy deep-dives
live in `.claude/rules/*.md` and are read **on demand** — this rulebook links
them explicitly (never rely on nested auto-loading).

**Scope fence** — keep the groups separate:

- **G1 — Orientation**: the *terrain* (mental model, layers, parity ledger, files).
- **G2 — Porting workflow**: *how to add* a feature (the C-ABI-first spine).
- **G3 — FFI marshalling & ownership**: the *rules you must not break* (P/Invoke,
  handles, zero-copy, errors, callbacks, threading, loading) —
  `.claude/rules/ffi-marshalling.md`.
- **G4 — API shape & design**: naming, the Java-shape target, open decisions.
- **G5 — Build / run / verify**: TFM matrix, commands, local broker, tests.
- **G6 — Governance & review**: the .NET Actor / Critic personas.

## Roadmap

| Group | Status | Lives in |
|---|---|---|
| G1 Orientation | ✅ below | this file |
| G2 Porting workflow | ✅ below | this file |
| G3 FFI marshalling & ownership | ✅ done | `.claude/rules/ffi-marshalling.md` |
| G4 API shape & design | ✅ below | this file |
| G5 Build / run / verify | ✅ below | this file |
| G6 Governance & review | ⏳ personas TBD | `.claude/agents/*.md` (not yet created) |
| **Binding code** | ❌ **not started** | `bindings/dotnet/src/…` (intended) |

---

# G1 · Orientation — the mental model

Descriptive altitude: prose + tables + one worked trace. The
`Rule / Why / Anti-patterns` format starts in G3 (`ffi-marshalling.md`).

## 1.1 The four layers & the one law

The .NET binding is a **P/Invoke wrapper** over the Rust core's **C ABI**.
`bindings/CLAUDE.md §1` states the generic four-layer model; here it is concrete:

| Layer | Real file(s) | Holds | Cannot express |
|---|---|---|---|
| **Java client** | reference (Apache Kafka 4.2, not in repo) | the API *shape* — the target | — |
| **Rust core** | `src/producer/…`, `src/consumer/…` | **all Kafka logic** (batching, partitioning, retries, network) | — |
| **C ABI** | `src/ffi/producer.rs` → generated `confluent_kafka.h` | flat `extern "C"` fns + opaque `*_t` handles | generics, `async`, exceptions, overloads |
| **.NET binding** | `Native` P/Invoke class + `SafeHandle`s + managed `Producer` (intended) | restores the Java shape | — |

**The one law:** *a binding restores the Java **shape**; it holds **no Kafka
logic** — that lives once, in the Rust core.* Every line of the .NET binding is
one of exactly two things, never a third:

- **Shape** — the Java-shaped surface (`Producer.SendAsync(ProducerRecord) →
  Task<RecordMetadata>`, `KafkaException` with `Code`/`IsRetriable`/`IsFatal`).
- **Scaffolding** — glue that wraps the ABI safely (`SafeHandle` cleanup, the
  completion pump, byte pinning, `Utf8` helpers).
- **NOT Kafka logic** — no partitioning, batching, retries, offset tracking. If
  you're writing Kafka behavior in C#, you're in the wrong layer.

## 1.2 One call, end to end

`producer.SendAsync(record)` travels down all four layers; the result travels
back up (mechanics → G3):

```
.NET:   producer.SendAsync(record) -> Task<RecordMetadata>
  │      validate args (precondition throw); pin key/value; make TaskCompletionSource
  ▼
P/Invoke: Native.kafka_producer_Producer_send(handle, topicPtr, …, out err) -> future handle
  │      the completion pump enqueues (future, tcs); blocks on get_all(); completes each tcs
  ▼
C ABI:  kafka_producer_Producer_send / _get_all(futures[], metadata[], errors[])
  │      src/ffi/producer.rs: the extern "C" boundary — flat pointers, out-params
  ▼
Rust:   Producer::send(ProducerRecord) -> KafkaFuture<RecordMetadata>
          the real accumulator / sender / single-Selector network stack — ALL the logic

  ▲  completion travels back up the same ladder:
  │  KafkaFuture resolves -> RecordMetadata_t* (or KafkaError_t*, null = success)
  └─ -> pump reads fields + frees handles -> tcs.SetResult / SetException
```

The Java *shape* (`send → Task of metadata`) survives the round trip though C in
the middle expresses none of it. `async` in particular is flattened at the ABI
(a blocking `get`) and rebuilt as a `Task` (→ G3 §7).

## 1.3 Two dialects at the boundary

Two *kinds* of type cross, deliberately:

**Opaque handles** — pointer-only tokens (`IntPtr` in C#); each read is a
function call; each has a `_destroy`. The C-visible `_t` is a zero-sized
placeholder over a hidden Rust type:

| C handle (`_t`) | Hidden Rust type | .NET holds it as |
|---|---|---|
| `kafka_producer_Producer_t` | `Mutex<ProducerKind>` | `SafeProducerHandle` (long-lived, §G3-2) |
| `kafka_producer_FutureRecordMetadata_t` | `FfiFuture` | `IntPtr` (transient, pump-owned) |
| `kafka_producer_RecordMetadata_t` | `RecordMetadataInner` | `IntPtr` (transient) |
| `kafka_common_KafkaError_t` | `KafkaErrorInner` | `IntPtr` (transient) |
| `kafka_producer_ProducerProperties_t` | `HashMap<String,String>` | `IntPtr` / `SafeHandle` (construction only) |

**Transparent struct** — `kafka_producer_ProducerRecord_t` has *visible*
fixed-width fields (`topic, partition, timestamp, key, key_len, value,
value_len`) with sentinels (`-1` = unset, null = absent). Marshalled as
`[StructLayout(LayoutKind.Sequential)]`; the binding fills it and pins the
key/value buffers so Rust borrows them zero-copy.

**Takeaway:** state you don't own → opaque handle (`IntPtr` / `SafeHandle`); data
you hand in → transparent struct. Who frees what, and buffer lifetime → **G3**.

## 1.4 The C ABI is generated & curated

`confluent_kafka.h` **is not checked in**. `cargo build --features ffi` runs
cbindgen (via `build.rs`) and emits `target/include/confluent_kafka.h`. The
surface is curated (`cbindgen.toml` `[export].include` lists only FFI types).
**Source of truth = `src/ffi/*.rs` + `cbindgen.toml`**, not a header you can grep
in a fresh clone.

## 1.5 Feature-parity ledger

Shared with all bindings — the ABI is the same file:

| Capability | Rust core | C ABI (`src/ffi`) | .NET |
|---|---|---|---|
| Producer (`send`/`flush`/`close`) | ✅ | ✅ | ❌ not built |
| MockProducer | ✅ | ✅ | ❌ not built |
| **Consumer (KIP-848)** | ✅ **fully built** | ❌ **not exposed** | ❌ unreachable |
| Transactions / Admin | partial | ❌ | ❌ |

The exposed C surface is six families (~30 functions; `src/ffi/producer.rs` is
authoritative): **Producer lifecycle** (`KafkaProducer_new`, `Producer_send`,
`_send_batch`, `_flush`, `_close`, `_destroy`), **Properties** (`_new`/`put`/
`from_configs`/`destroy`), **Future** (`_get`/`get_all`/`is_done`/`destroy`/
`destroy_all`), **RecordMetadata** (`offset`/`partition`/`timestamp`/`topic`/
`copy`/`destroy`), **KafkaError** (`code`/`message`/`is_retriable`/`is_fatal`/
`destroy`), **MockProducer** (`new`/`complete_next`/`error_next`/`history_count`/
`clear`).

**The consumer gap defines the work.** So "port a Rust feature to .NET" splits:
feature already at the ABI (producer family) → **.NET-only work**; feature only
in the Rust core (consumer, txns) → **build the C-ABI layer first**, then .NET.
That split is the subject of **G2**.

> **Keep this ledger current.** Whenever the ABI changes, re-derive from
> `src/ffi/*.rs` — a stale ledger misroutes the first decision a port makes.
> (This table is duplicated in the Python rulebook against the same ABI; update
> both.)

## 1.6 File map (intended)

| Concern | Location (intended) |
|---|---|
| .NET API (shape) | `bindings/dotnet/src/…/Producer.cs`, `KafkaException.cs`, `RecordMetadata.cs` |
| P/Invoke bridge | `bindings/dotnet/src/…/Native.cs`, the `SafeHandle`s, `Utf8` helpers |
| Build wiring | `bindings/dotnet/src/…/*.csproj` (multi-TFM + native-copy MSBuild step) |
| Tests | `bindings/dotnet/tests/` (MockProducer, no broker) |
| **C ABI (source of truth)** | `src/ffi/producer.rs`, `src/ffi/mod.rs` |
| ABI header config | `cbindgen.toml`, `build.rs` → `target/include/confluent_kafka.h` (generated) |
| Raw-ABI sibling binding | `bindings/c/` (the "degenerate" binding — *is* the ABI) |
| Python sibling (shape reference) | `bindings/python/producer.py` (a real, working restoration) |

## 1.7 Orientation facts you can rely on

Always true; the enforcing *rule* lives in G3:

- **Null `kafka_common_KafkaError_t` = success** (error out-param convention).
- **Every opaque handle has a `_destroy`; the current owner must call it exactly
  once** (`Box::into_raw` in Rust, reclaimed by `Box::from_raw`).
- **Output strings are borrowed** from a cached `CString` inside the handle —
  copy them (via `Utf8.PtrToString`) before the handle is freed.
- **`async` is flattened then rebuilt** — the ABI exposes a blocking `get`; the
  binding rebuilds a `Task` with a completion pump (→ G3 §7).
- **The binding is shape + scaffolding only** — never Kafka logic (1.1).

---

# G2 · Porting Workflow — how to add a feature

Procedural altitude: a decision gate, the step loops, copy-me shapes. Every
enforceable **rule** is referenced where it bites and stated in **G3**
(`ffi-marshalling.md`). The producer (once built) is the reference; the Python
binding is a working cross-language reference for the same ABI.

## 2.1 The decision gate

```
Is the feature already exposed at the C ABI (src/ffi)?
        │
   yes ─┤→ MODE A · .NET-only    (Native decl + SafeHandle + managed wrapper)        → 2.2
        │
   no ──┘→ MODE B · Full-stack   (src/ffi → header → Native → managed API)           → 2.3
```

Mode A is mechanical wrapping. Mode B is real engineering — the work is the ABI
design, not the C#.

## 2.2 Mode A — .NET-only port

The `kafka_*` function already exists in the header:

1. **P/Invoke** — add the `[DllImport]` declaration to `Native` (2.4, G3 §1).
2. **Ownership** — a `SafeHandle` subclass for any new long-lived handle (G3 §2);
   transient handles are read-and-freed, not wrapped.
3. **Managed API** — the Java-shaped method in `Producer.cs`; marshal
   strings/bytes (G3 §3–4), map errors (G3 §5), bridge async to `Task` (G3 §7).
4. **Test** — against `MockProducer`, no broker (G5).

## 2.3 Mode B — full-stack port (the C-ABI-first loop)

The feature lives only in the Rust core. Walk all four layers, ABI first:

1. **Design the ABI surface** — opaque handles, transparent structs, functions
   (naming per 2.5). *This is the real work.*
2. **Write `src/ffi/<area>.rs`** — the four archetypes (2.4); `pub mod <area>;`.
3. **Make the types emit** — add zero-field opaque `*_t` structs to
   `cbindgen.toml` `[export].include`.
4. **Regenerate** — `cargo build --features ffi`; confirm the symbols land in
   `target/include/confluent_kafka.h`.
5. **Wrap in `Native`** — `[DllImport]` declarations + `SafeHandle`s.
6. **Expose the managed API** — the Java-shaped surface in a new class.
7. **Test & build** — Mock/parity tests → `dotnet build` → `dotnet test`.

## 2.4 Per-layer archetypes (the copy-me toolkit)

**C ABI** — every `extern "C"` fn is one of four shapes (canonical:
`src/ffi/producer.rs`): constructor, action, getter, destroy.

**.NET bridge** — the pieces (details in G3):

```csharp
// (1) P/Invoke declaration (§1) — one uniform set, Cdecl.
[DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]
internal static extern IntPtr kafka_producer_Producer_send(/* … */, out IntPtr outError);

// (2) SafeHandle for a long-lived handle (§2).
sealed class SafeProducerHandle : SafeHandle {
    public SafeProducerHandle() : base(IntPtr.Zero, ownsHandle: true) { }
    public override bool IsInvalid => handle == IntPtr.Zero;
    protected override bool ReleaseHandle() { Native.kafka_producer_Producer_destroy(handle); return true; }
}

// (3) Managed wrapper — restore the Java shape (§7 async, §5 errors, §3–4 marshalling).
public Task<RecordMetadata> SendAsync(ProducerRecord record) { /* pin, call, enqueue on pump */ }
```

## 2.5 Cross-layer conventions

**Naming** — drop `clients`; the shape stays recognizable at every layer:

| Java | Rust core | C ABI | .NET (managed) |
|---|---|---|---|
| `KafkaProducer.send()` | `Producer::send()` | `kafka_producer_Producer_send` | `Producer.SendAsync()` |
| `RecordMetadata.offset()` | `RecordMetadata::offset()` | `kafka_producer_RecordMetadata_offset` | `RecordMetadata.Offset` |
| `KafkaException` (hierarchy) | `KafkaError` | `kafka_common_KafkaError_t` | `KafkaException` (flat, §5) |

Pattern: `kafka_<pkg-minus-clients>_<Type>_<method>` at the ABI; **C# casing**
above it (PascalCase methods, properties for Java getters, `Async` suffix for
`Task`-returning methods — see the G4 naming decision).

**Handle protocol** — opaque `*_t` handles cross as `IntPtr`; long-lived ones are
owned by a `SafeHandle`, transient ones read-and-freed on the pump (G3 §2). Never
model an opaque `_t` as a C# struct.

## 2.6 Decisions every port must make

| Decision | Guidance / default | Rule lives in |
|---|---|---|
| Thin pass-through vs. binding machinery | Default **thin**. Add binding-side state (the completion pump) *only* as scaffolding — **never Kafka logic**. | G3 §7 |
| New `src/ffi` module | `src/ffi/<area>.rs` + `pub mod <area>;`. | 2.3 |
| cbindgen emission | Zero-field opaque `*_t` → `cbindgen.toml` `[export].include`. | 2.3 step 3 |
| Serializer / deserializer | ABI is bytes-only (`ByteArraySerializer`); (de)serialize on the .NET side. | G4 |
| Precondition checks | Validate in the managed layer **before** the P/Invoke and throw .NET argument/state exceptions — the ABI does not validate and may panic. | G3 §5 |

## 2.7 ⚠ Before you start Mode B for the consumer

The producer send path hands bytes *in* and gets a small metadata handle back —
no borrowed-bytes-out problem. The **consumer is the opposite**: the receive-path
zero-copy contract (`consumer-threading.md §27`) says fetched bytes are owned by
one buffer and every record borrows a slice. Crossing those **borrowed slices**
through a C ABI into .NET — which wants owned `byte[]` / `ReadOnlyMemory<byte>` —
is the crux design problem, and it does not exist on the send path. Resolve the
lifetime/ownership model *before* designing the consumer ABI.

---

# G3 · FFI marshalling & ownership

The boundary correctness rules — P/Invoke declarations, handle ownership
(`SafeHandle`), UTF-8 marshalling, zero-copy pinning, the error model, callbacks,
threading, and native loading — live in **`.claude/rules/ffi-marshalling.md`**
(read on demand). G2 references its sections as "G3 §N". This rulebook does not
restate them.

---

# G4 · API shape & design

G4 states the **shape target** (firm) and the **design decision points** a port
resolves — each with a recommended default. Per repo precedent
(`consumer-threading.md §28`), the rulebook gives defaults and requires a
rationale for deviations; the implementing agent makes each call in context and
records why (phase PLAN, a COMMENTS.DONE entry, or a code comment).

## 4.1 The shape target (firm)

- **Mirror the Java public API shape** — `Producer` → `Send(ProducerRecord) →
  Future<RecordMetadata>` (as a .NET `Task`), getter-style `RecordMetadata`,
  `MockProducer`. The consumer will mirror `KafkaConsumer`
  (`poll`/`subscribe`/`commit`).
- **Do NOT adopt the ecosystem client's shape.** No `confluent-kafka-dotnet`
  `IProducer<TKey,TValue>` / `ProduceAsync` / delivery-report handler / `Message<K,V>`
  model. Target the *Java* client, per `bindings/CLAUDE.md §2`.
- **C# casing:** PascalCase types and methods; Java getters (`offset()`) become
  properties (`Offset`); keep the Java method names otherwise (`Flush`, `Close`,
  `PartitionsFor`).
- **Lifecycle:** mirror Java `AutoCloseable` / `close()` as .NET disposal
  (`IDisposable` / `IAsyncDisposable`) — the exact form is a 4.2 decision. The
  public `Producer` is thread-safe (Java's contract; enforced by the core
  `Mutex`, G3 Thread topology), so its methods are callable from any thread.

## 4.2 Design decision points (resolve at implementation, record rationale)

| Decision point | Recommended default | Decide when |
|---|---|---|
| **Async method naming** | `SendAsync` (the `Async` suffix is a near-universal C# convention for `Task`-returning methods; the shared convention permits host-idiom adaptation). `ffi-marshalling.md` assumes this. Deviation: strict-Java `Send`. | first `Task`-returning method |
| Serializer / deserializer | A .NET-side `ISerializer<T>` / `IDeserializer<T>` producing/consuming `byte[]`; the ABI stays bytes-only. Raw-bytes-only is an acceptable interim. No per-record serializer callback through the ABI (hot-path C↔.NET, against `CLAUDE.md §11`). | porting (de)serialization |
| Interceptors | Defer; reserve the Java-shaped name so adding it later is non-breaking. | a concrete user need |
| Config value coercion | Coerce non-string config values to `str` in the .NET layer (accept ints / bools), matching Java ergonomics. | wiring the constructor |
| Error granularity | One flat `KafkaException` for now (G3 §5); typed subclasses can be added later, non-breakingly. | if catch-by-type is needed |
| **Disposal model** | Implement **both**: `IAsyncDisposable.DisposeAsync()` as the primary graceful close (joins the pump + `flush`/`close` without blocking, G3 §2/§7), and `IDisposable.Dispose()` as a blocking fallback for `using` in sync code. Java `close(Duration)` → `CloseAsync(TimeSpan)`. | first client type |
| **`CancellationToken`** | Accept one on async methods (`SendAsync`/`FlushAsync`/`CloseAsync`) — idiomatic .NET. Honored **best-effort** (cancels the *wait*, per G3 §7; does **not** abort an already-enqueued send). A host-idiom addition Java lacks — allowed by `bindings/CLAUDE.md §2`. Deviation: strict-Java, no token. | first async method |
| **Namespace / package id** | A namespace **distinct from `Confluent.Kafka`** — that root is owned by the ecosystem `confluent-kafka-dotnet` package and collides (its `IProducer<TKey,TValue>` clashes visually too). Decide before declaring any public type. | before the first public type |
| **Public key/value type** | `ReadOnlyMemory<byte>` for `ProducerRecord` key/value (zero-copy-friendly; pins via `MemoryHandle`, G3 §4), with `byte[]` implicitly usable. `byte[]`-only is an acceptable interim. | porting `ProducerRecord` |
| **Nullable reference types** | `#nullable enable` project-wide; annotate the P/Invoke surface precisely (non-null `IntPtr` vs nullable out-params). | project setup |

## 4.3 Config mapping (mechanism — firm)

- A config `IDictionary<string,string>` (or `IEnumerable<KeyValuePair<…>>`) →
  per-entry `ProducerProperties_put(k, v)`; keys are the **Java dotted names**
  (`bootstrap.servers` required). Values follow the 4.2 coercion decision.
- Classic-/consumer-only keys are **accepted silently** (repo policy — match Java,
  no Rust-side rejection).
- `key.serializer` / `value.serializer` follow the 4.2 serializer decision (today
  the ABI pins `ByteArraySerializer`, so they are inert).

---

# G5 · Build / run / verify

The build order, commands, running against a broker, and test conventions.

## 5.1 The build is a two-stage pipeline: Rust → .NET (firm)

The .NET binding cannot run until the Rust side has produced the native library:

```
cargo build --features ffi [--release]                    (build-rust)
   └─ target/<profile>/{lib}confluent_kafka.{so,dylib,dll}  ← the binding P/Invokes this
                    │  (must exist first)
                    ▼
dotnet build   (an MSBuild step copies the native into $(OutDir))  (build-dotnet)
```

The native is resolved by default `[DllImport]` probing once copied beside the
managed assembly (G3 §8). Never build .NET before Rust — the native won't exist.

## 5.2 Commands (intended)

| Goal | Command |
|---|---|
| Build the native | `cargo build --features ffi [--release]` |
| Build the binding | `dotnet build` (copies the native to output) |
| Unit tests (**no broker**) | `dotnet test` (MockProducer) |
| Rust tests | `cargo test` |
| Format / lint (Rust) | `cargo xtask format` / `cargo xtask lint` |
| Format (C#) | `dotnet format` |

## 5.3 Running against a broker

- **No broker** — `MockProducer` (unit tests; also how to iterate without infra).
- **Your own local broker** — `bootstrap.servers` is just a config key.
- **Integration** — spin a broker via testcontainers (needs Docker), not a
  checked-in compose file.

## 5.4 Test conventions

- Unit tests hold a `MockProducer` (auto- or manual-complete via
  `complete_next`/`error_next`), and `await` the returned `Task` with a timeout —
  the timeout doubles as the **pump-join / deadlock regression guard** (G3 §7).
- **TFM-matrix smoke test**: the binding loads and a `MockProducer` round-trips on
  **net462** (via netstandard2.0), **net8.0**, **net10.0** (G3 §1, §8).
- Parity obligations (`definition-of-done.md §3`): mirror the Java/Rust tests,
  **assert error-message content** (G3 §5), and add a per-record
  **allocation-budget** test on the send path (G3 §4, DoD §10).

## 5.5 Packaging & versions (decision points — resolve when we publish)

| Decision point | Recommended default | Decide when |
|---|---|---|
| TFMs | `netstandard2.0` (floor, covers net462) + `net8.0` + `net10.0` (G3 §1). | now |
| Native distribution | Copy-to-output for dev/test now; a separate redist NuGet (`runtimes/{rid}/native/`) later — packaging-only (G3 §8). | preparing distribution |
| Assembly signing / package id | Defer to distribution. | preparing distribution |
| NativeAOT / trimming | Not now; kept viable by using direct `[DllImport]` (G3 §1, §8). | if a concrete need surfaces |

## 5.6 Definition of done

A port is not done until it builds on the TFM matrix, unit tests pass against
`MockProducer`, lint/format are clean, and the G3 anti-patterns are satisfied
(`definition-of-done.md`). Integration/multi-language suites are opt-in until
CI-stable.

---

# G6 · Governance & review

Who builds and reviews this binding. The *process* is inherited from root
`agent-roles.md`; the personas add .NET review expertise. The review *criteria*
live in G3 (anti-patterns) and G5 (verify) — G6 points at them.

## 6.1 Personas (to be created)

- **`dotnet-actor`** and **`dotnet-critic`** (`.claude/agents/*.md`) will inherit
  the Actor / Critic roles and the `COMMENTS.<N>.md` loop from `agent-roles.md`.
  The Manager is the root `project-manager` (coordination is client-agnostic).
- They are needed because the root `actor-executor` / `kafka-critic` are
  Rust-translation-shaped and don't know P/Invoke / .NET interop.

## 6.2 Review ground truth (firm)

Review a change against the **C ABI header** (`confluent_kafka.h`) and the **Kafka
Java public API shape** — **not** Rust internals, and **not** Java implementation
logic (`bindings/CLAUDE.md §2`).

## 6.3 The Critic's lens (.NET-specific)

`SafeHandle` / `Dispose` correctness · handle leak / double-free / use-after-free ·
byte pinning & buffer lifetime · `MarshalAs(I1)` for `bool` · UTF-8 (no `LPStr`) ·
no managed exception through a callback · `RunContinuationsAsynchronously` on the
pump · flat `KafkaException` vs precondition .NET exceptions · **shape, not
logic**. The concrete checklist is the **Anti-patterns** blocks in G3 and the
decision tables in G2/G4.

## 6.4 Mechanics

- Review comments: `bindings/dotnet/COMMENTS.<N>.md`; resolved →
  `COMMENTS.DONE.<N>.md`.
- Agent memory: `bindings/dotnet/.claude/agent-memory/<persona>/`.
- ⚠ **Nested-agent discovery is unverified** — if the harness does not
  auto-register `bindings/dotnet/.claude/agents/*.md`, place copies under the
  repo-root `.claude/agents/` or invoke with the persona files loaded explicitly.
