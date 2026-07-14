# Confluent Kafka Python Binding — Claude Rules

**What this file is.** The concrete rulebook for the Python binding under
`bindings/python/`. It is the Python-specific instance of the shared,
language-agnostic binding rulebook `bindings/CLAUDE.md` (the 4-layer mental
model + cross-binding conventions), which is itself governed by the repo-root
`CLAUDE.md` (Rust-core translation rules). This file holds the *concrete*
realization — real files, real `*_t` types, the real call path — never a
re-statement of the generic model.

**How it loads.** Working anywhere under `bindings/python/` stacks three
auto-loaded rulebooks: root `CLAUDE.md` → `bindings/CLAUDE.md` → this file.
Heavy deep-dives live in `.claude/rules/*.md` and are read **on demand** —
this rulebook links them explicitly (never rely on nested auto-loading).

> The shared `bindings/CLAUDE.md` arrives with the bindings initiative
> (PR #120). Until it lands on your branch, references to "the shared
> rulebook" point at that incoming file.

**Scope fence.** Keep the groups separate:

- **G1 — Orientation** (this section): the *terrain*.
- **G2 — Porting workflow**: *how to add* a feature (the C-ABI-first spine).
- **G3 — FFI boundary contracts**: the *rules you must not break* (GIL,
  ownership, zero-copy, errors, async) — `.claude/rules/python-ffi.md`.
- **G4 — API shape & design**: naming, Java-shape target, open decisions.
- **G5 — Build / run / verify**: commands, local broker, tests.
- **G6 — Governance & review**: the Python Actor/Critic personas.

## Roadmap

| Group | Status | Lives in |
|---|---|---|
| G1 Orientation | ✅ below | this file |
| G2 Porting workflow | ✅ below | this file |
| G3 FFI boundary contracts | ✅ done | `.claude/rules/python-ffi.md` |
| G4 API shape & design | ▢ forthcoming | this file (+ a rule file if it grows) |
| G5 Build / run / verify | ▢ forthcoming | this file |
| G6 Governance & review | ▢ forthcoming | `.claude/agents/*.md` |

---

# G1 · Orientation — the mental model

Orientation altitude: this section is *descriptive* (prose + tables + one
worked trace). It introduces concepts and routes every enforceable rule
forward to G2/G3. The `Rule / Why / How / Anti-patterns` format starts at G3.

## 1.1 The four layers & the one law

The Python binding is a hand-written **CPython C extension** over the Rust
core's **C ABI**. `bindings/CLAUDE.md §1` states the generic four-layer model;
here it is made concrete against the files in this repo:

| Layer | Real file(s) | Holds | Cannot express |
|---|---|---|---|
| **Java client** | reference (Apache Kafka 4.2, not in repo) | the API *shape* — the target | — |
| **Rust core** | `src/producer/…`, `src/consumer/…` | **all Kafka logic** (batching, partitioning, retries, network) | — |
| **C ABI** | `src/ffi/producer.rs` → generated `confluent_kafka.h` | flat `extern "C"` fns + opaque `*_t` handles | generics, `async`, exceptions, overloads, methods |
| **Python binding** | `bindings/python/_confluentkafka.c` (C ext) + `producer.py` | restores the Java shape | — |

**The one law:** *a binding restores the Java **shape**; it holds **no Kafka
logic** — that lives once, in the Rust core.* Every line in `producer.py` /
`_confluentkafka.c` is one of exactly two things, never a third:

- **Shape** — the Java-shaped surface (`Producer.send() → Future[RecordMetadata]`,
  `KafkaError` with `code`/`is_retriable`/`is_fatal`).
- **Scaffolding** — glue that wraps the ABI safely (GIL handoff, handle
  bookkeeping, the `self.futures` tracking set).
- **NOT Kafka logic** — no partitioning, batching decisions, retries, offset
  tracking. If you're writing Kafka behavior in Python or C, you're in the
  wrong layer.

## 1.2 One call, end to end

The anchor mental model. `producer.send(record)` travels down all four layers
and the result travels back up:

```
Python:   producer.send(record) -> concurrent.futures.Future
  │        producer.py: validate, make Future, define cb(result, error), track in self.futures
  ▼
C ext:    _lib.Producer_send(c_producer, record, cb)
  │        _confluentkafka.c: enqueue into a batch slot; a send_thread drains it and calls
  │        the ABI; a poll_futures_thread blocks on completion WITHOUT the GIL, then
  │        re-acquires the GIL to fire cb with integer handles          (mechanics → G3)
  ▼
C ABI:    kafka_producer_Producer_send_batch(producer, records[], futures[], errors[], n)
  │        src/ffi/producer.rs: the extern "C" boundary — flat pointers, out-params
  ▼
Rust:     Producer::send(ProducerRecord) -> KafkaFuture<RecordMetadata>
           the real accumulator / sender / network stack — ALL the logic

  ▲  completion travels back up the same ladder:
  │  KafkaFuture resolves -> RecordMetadata_t* (or KafkaError_t*, null = success)
  │  -> Python cb -> RecordMetadata._from_c / KafkaError._from_c
  └─ -> future.set_result(...) / future.set_exception(...)
```

The Java *shape* (`send → Future of metadata`) survives the round trip even
though C in the middle expresses none of it. `async` in particular is
destroyed at the ABI and rebuilt (see 1.7).

## 1.3 Two dialects at the boundary

Not everything is an opaque handle — two *kinds* of type cross, and the choice
is deliberate:

**Opaque handles** — pointer-only tokens. C/Python can't see inside; every
read is a function call; each has a `_destroy`. The C-visible `_t` is a
zero-sized placeholder, and the real object is a *hidden* Rust type behind it:

| C handle (`_t`) | Hidden Rust type behind the pointer |
|---|---|
| `kafka_producer_Producer_t` | `Mutex<ProducerKind>` (`Mock` \| `Kafka`) |
| `kafka_producer_FutureRecordMetadata_t` | `FfiFuture { future, runtime_handle }` |
| `kafka_producer_RecordMetadata_t` | `RecordMetadataInner { metadata, topic_cstring }` |
| `kafka_common_KafkaError_t` | `KafkaErrorInner { error, message_cstring }` |
| `kafka_producer_ProducerProperties_t` | `HashMap<String, String>` |

**Transparent struct** — `kafka_producer_ProducerRecord_t` has *visible* fields
(`topic, partition, timestamp, key, key_len, value, value_len`), fixed-width
for portability, with sentinels (`-1` = unset, null = absent). The C ext fills
it **in place**, and Rust borrows the buffers with `slice::from_raw_parts` —
that borrow *is* the zero-copy crossing.

**Takeaway:** state you don't own → opaque handle; data you hand in →
transparent struct. Who frees what, and how long the borrowed buffer must live
→ **G3**.

## 1.4 The C ABI is generated & curated

`confluent_kafka.h` **is not checked in**. `cargo build --features ffi` runs
cbindgen (via `build.rs`) and emits `target/include/confluent_kafka.h`. The
surface is **curated**: `cbindgen.toml`'s `[export].include` lists only FFI
types, so internal Kafka constants/structs never leak into C.

**Source of truth for the ABI = `src/ffi/*.rs` + `cbindgen.toml`**, not a
header you can grep in a fresh clone. Adding a feature means adding an
`extern "C"` fn there *and* confirming it emits correctly under these cbindgen
rules.

## 1.5 Feature-parity ledger

The three columns do not line up — and the gap is the whole point of the
porting effort:

| Capability | Rust core | C ABI (`src/ffi`) | Python |
|---|---|---|---|
| Producer (`send`/`flush`/`close`) | ✅ | ✅ | ✅ |
| MockProducer | ✅ | ✅ | ✅ |
| **Consumer (KIP-848)** | ✅ **fully built** | ❌ **not exposed** | ❌ unreachable |
| Transactions / Admin | partial | ❌ | ❌ |

The exposed C surface is six families (~30 functions — `src/ffi/producer.rs`
is authoritative):

- **Producer lifecycle** — `KafkaProducer_new`, `Producer_send`,
  `Producer_send_batch`, `Producer_flush`, `Producer_close`, `Producer_destroy`
- **Properties** — `ProducerProperties_new` / `put` / `from_configs` / `destroy`
- **Future** — `FutureRecordMetadata_get` / `get_all` / `is_done` / `destroy` /
  `destroy_all`
- **RecordMetadata** — `RecordMetadata_offset` / `partition` / `timestamp` /
  `topic` / `copy` / `destroy`
- **KafkaError** — `KafkaError_code` / `message` / `is_retriable` / `is_fatal` /
  `destroy`
- **MockProducer** — `MockProducer_new` / `complete_next` / `error_next` /
  `history_count` / `clear`

**The consumer gap defines the work.** The KIP-848 consumer is fully built in
Rust but invisible to Python because `src/ffi/` never exported it. So "port an
existing Rust feature to Python" splits in two:

- feature already at the ABI (producer family) → **Python-only work**;
- feature only in the Rust core (consumer, txns) → **build the entire C-ABI
  layer first**, then Python. That split is the subject of **G2**.

> **Keep this ledger current.** Counts here are indicative; whenever the ABI
> changes, re-derive from `src/ffi/*.rs` and update this table. A stale ledger
> misroutes the very first decision a porting task makes.

## 1.6 File map

| Concern | Location |
|---|---|
| Python API (shape) | `bindings/python/producer.py` |
| C extension (bridge) | `bindings/python/_confluentkafka.c` |
| Build wiring | `bindings/python/setup.py`, `Makefile`, `pyproject.toml` |
| Tests | `bindings/python/test/unit/` (MockProducer), `test/performance/` |
| gRPC perf server | `bindings/python/grpc_server.py`, `Dockerfile.grpc` |
| **C ABI (source of truth)** | `src/ffi/producer.rs`, `src/ffi/mod.rs` |
| ABI header config | `cbindgen.toml`, `build.rs` → `target/include/confluent_kafka.h` (generated) |
| Raw-ABI sibling binding | `bindings/c/` (the "degenerate" binding — *is* the ABI) |
| Rust core (logic) | `src/producer/…`, `src/consumer/…` |

## 1.7 Orientation facts you can rely on

Always true; the *rule* that enforces each one lives in G3:

- **Null `kafka_common_KafkaError_t` = success.** The error-out-param
  convention: null means "no error." (→ G3 error model.)
- **Every opaque handle has a `_destroy`, and the current owner must call it.**
  Handles are `Box::into_raw`'d in Rust and reclaimed by `Box::from_raw` in the
  matching `_destroy`. (→ G3 ownership.)
- **Strings cross as a cached `CString` inside the handle** (`topic_cstring`,
  `message_cstring`), so a `*const c_char` getter returns a stable pointer.
- **`async` is flattened then rebuilt.** The ABI bundles the tokio
  `runtime_handle` into `FfiFuture` and exposes a blocking `get`
  (`runtime.block_on`); the C ext calls it off-GIL; Python re-wraps it as a
  `concurrent.futures.Future`. (→ G3 async model.)
- **The binding is shape + scaffolding only** — never Kafka logic (1.1).

**What G1 deliberately excludes:** the C-ABI-first porting steps (→ G2); all
ownership / GIL / lifetime *rules* (→ G3); config-key mapping and
serializer/interceptor decisions (→ G4); build commands beyond "the header is
generated" (→ G5).

---

# G2 · Porting Workflow — how to add a feature

Procedural altitude: this section is the *runbook* — a decision gate, the step
loops, and copy-me shapes. Every enforceable **rule** (GIL, refcount, ownership,
zero-copy, error model) is referenced at the step where it bites and stated in
**G3** (`.claude/rules/python-ffi.md`). **The producer is your reference
implementation** — G2 shows shapes and points at the real functions; copy
those, don't reinvent.

## 2.1 The decision gate

Every port starts at G1's parity ledger (§1.5) with one question:

```
Is the feature already exposed at the C ABI (src/ffi)?
        │
   yes ─┤→ MODE A · Python-only   (2 files:  _confluentkafka.c + producer.py)        → 2.2
        │
   no ──┘→ MODE B · Full-stack    (4 layers: src/ffi → header → C ext → producer.py) → 2.3
```

Mode A is mechanical wrapping. Mode B is real engineering — the work is the ABI
design, not the Python.

## 2.2 Mode A — Python-only port

The feature's `kafka_*` function already exists in the header. Two files:

1. **C ext** — add a `py_*` wrapper (2.4) calling the existing ABI function, and
   register it in `ProducerNativeMethods[]`.
2. **Python** — add the Java-shaped method in `producer.py`; wrap any returned
   handle with a `_from_c(_id)` class; bridge results to `Future` / `KafkaError`.
3. **Test** — unit-test against `MockProducer` (no broker).
4. **Build & verify** — `cd bindings/python && make build`, then pytest.

## 2.3 Mode B — full-stack port (the C-ABI-first loop)

The feature lives only in the Rust core. Walk all four layers, ABI first:

1. **Design the ABI surface** — decide the opaque handles, transparent structs,
   and functions (naming per 2.5). *This is the real work.*
2. **Write `src/ffi/<area>.rs`** — implement the four archetypes (2.4) per type;
   add `pub mod <area>;` to `src/ffi/mod.rs`.
3. **Make the types emit** — add zero-field opaque `*_t` structs to
   `cbindgen.toml` `[export].include` (types used in signatures come along
   automatically).
4. **Regenerate** — `cargo build --features ffi`; confirm the new symbols land
   in `target/include/confluent_kafka.h`.
5. **Wrap in the C ext** — `py_*` wrappers + method-table entries + a
   `PyTypeObject` for any new Python type.
6. **Expose in Python** — the Java-shaped API in a new module (e.g.
   `consumer.py`) + `_from_c` handle wrappers.
7. **Test & build** — Mock/parity tests → `make build` → pytest.

## 2.4 Per-layer archetypes (the copy-me toolkit)

**C ABI — every `extern "C"` fn is one of four shapes** (canonical file:
`src/ffi/producer.rs`):

```rust
// (1) Constructor — handle in, real object built, handle out.   [KafkaProducer_new]
pub unsafe extern "C" fn kafka_<pkg>_<Type>_new(
    args…, out_error: *mut *mut kafka_common_KafkaError_t) -> *mut …_t {
    // build the real Rust object → Box::into_raw(Box::new(inner)) as *mut …_t
    // on failure: *out_error = box_error(e); return null_mut()
}
// (2) Action  — read args, borrow bytes zero-copy, block_on the core, box result. [Producer_send]
// (3) Getter  — cast ref, read field, return scalar / cached CString.             [RecordMetadata_offset]
// (4) Destroy — null-safe drop(Box::from_raw(ptr as *mut Inner)).                  [Producer_destroy]
```

**C ext — thin wrapper shape** (canonical: `_confluentkafka.c::py_KafkaError_code`):

```c
static PyObject* py_<Name>(PyObject* self, PyObject* args) {
    unsigned long long ptr;                             // handle-as-int (2.5)
    PyArg_ParseTuple(args, "K", &ptr);
    kafka_<…>_t* h = (kafka_<…>_t*)(uintptr_t)ptr;      // int -> pointer
    return <Py…_From…>( kafka_<…>(h) );                  // ABI call -> Python value
}
// + one line in ProducerNativeMethods[]:  {"<Name>", py_<Name>, METH_VARARGS, "..."}
```

*Action wrappers that need C-side state (threads/queues) are the exception — see
2.6 and G3 for the GIL/refcount rules.*

**Python — restore the Java shape** (canonical: `producer.py`):

```python
class Thing:
    @staticmethod
    def _from_c(_id: int):          # adopt the handle
        self = Thing.__new__(Thing); self._id = _id; return self
    def __del__(self):              # release it
        _lib.Thing_destroy(self._id)
```

## 2.5 Cross-layer conventions

**Naming** — drop `clients`; the shape stays recognizable at every layer:

| Java | Rust core | C ABI | C ext (Python-visible) | `producer.py` |
|---|---|---|---|---|
| `KafkaProducer.send()` | `Producer::send()` | `kafka_producer_Producer_send` | `_lib.Producer_send` | `Producer.send()` |
| `RecordMetadata.offset()` | `RecordMetadata::offset()` | `kafka_producer_RecordMetadata_offset` | `_lib.RecordMetadata_*` | `RecordMetadata.offset()` |
| `KafkaException` (hierarchy) | `KafkaError` | `kafka_common_KafkaError_t` | `_lib.KafkaError_*` | `KafkaError(Exception)` |

Pattern: `kafka_<pkg-minus-clients>_<Type>_<method>`; the whole exception
hierarchy collapses to one `kafka_common_KafkaError_t`.

**Handle-as-int protocol** — Python can't hold a raw C pointer, so `*_t` handles
cross as integers: `"K"` in `PyArg_ParseTuple` inbound,
`(kafka_…_t*)(uintptr_t)ptr` to reconstitute, `PyLong_FromUnsignedLongLong`
outbound. The Python wrapper class owns the lifecycle via `_from_c(_id)` /
`_destroy(_id)`. **Every new handle type follows this.**

## 2.6 Decisions every port must make

| Decision | Guidance / default | Rule lives in |
|---|---|---|
| Thin pass-through **vs.** C-ext machinery | Default **thin**. Add C-side state (threads/queues, like send batching) *only* to amortize FFI/GIL crossings — it's scaffolding, **never Kafka logic**. | G3 §1 (GIL/threading) |
| New `src/ffi` module | Create `src/ffi/<area>.rs`; add `pub mod <area>;` to `mod.rs`. | — |
| cbindgen emission | Zero-field opaque `*_t` structs → add to `cbindgen.toml` `[export].include`. | 2.3 step 3 |
| tokio runtime | Smuggle a runtime handle into the object/handle (the `FfiFuture` precedent) so blocking `get`s have a runtime to drive. | G3 §6 (async) |
| Serializer / deserializer | Hardcode raw bytes (`ByteArraySerializer`), matching the current shape. | G4 |
| Precondition checks | Repo practice: **defensively null-check** required handles and return null + `out_error = InvalidRequest` (diverges from root `CLAUDE.md §3`; the actual code wins). | G3 §5 (errors) |

## 2.7 ⚠ Before you start Mode B for the consumer

The producer send path has no borrowed-bytes-out problem — you hand bytes *in*
and get a small metadata handle back. The **consumer is the opposite**: the
receive-path zero-copy contract (`consumer-threading.md §27`) says fetched bytes
are owned by one buffer and every record borrows a slice from it. Crossing those
**borrowed slices** through a C ABI into Python — which wants to own its `bytes`
— is the crux design problem, and it does not exist on the send path. Resolve
the lifetime/ownership model (G3 §3–4 + `consumer-threading.md §27`) *before*
designing the consumer ABI; it may force copies the producer path never needed.

**What G2 deliberately excludes:** the GIL / refcount / ownership / zero-copy /
error-model *rules* (→ G3); the serializer/interceptor decision (→ G4); deep
build / packaging / version-matrix (→ G5 — only the regen + `make build`
commands appear here); test-coverage conventions like allocation budgets and
parity depth (→ G5/G6 — G2 keeps only "test with Mock + parity" as a step).
