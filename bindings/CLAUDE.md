# Bindings — Shared Mental Model & Conventions

Shared, binding-layer rulebook for the high-level language clients under
`bindings/`. Auto-loads for work anywhere under `bindings/`. Holds the
*language-agnostic* mental model and cross-binding conventions; each
language's own translation rules live in `bindings/<lang>/CLAUDE.md`.

---

## 1. Languages based on the C ABI

_Bindings that wrap the Rust core's C ABI (`confluent_kafka.h`) and reconstruct
a high-level client on top of it._

### 1.1 Scope

Starter set:

- **Python** — see `bindings/python/`
- **.NET** — see `bindings/dotnet/`

_(C is the degenerate case — it **is** the ABI, so there is nothing to
reconstruct; out of scope for this section.)_

### 1.2 Mental model

1. **Every binding presents Apache Kafka's Java public-API shape.**
2. **A binding rebuilds only that *shape* — never the core logic.** The logic
   lives once, in the Rust core; the binding is a thin wrapper that restores the
   shape on top of it via the C ABI.

Keeping the shape identical across languages — while reimplementing none of the
behavior — is the entire point.

**This reconstruction is not a 1:1 mapping.** Beyond the Kafka-shaped API, each
binding also carries additional host-language scaffolding — code that exists
only to wrap the C ABI safely and present the shape idiomatically. This is
expected — the scaffolding only supports the shape and **adds no new Kafka
behavior**; that always stays in the Rust core.

Follow the shape through the **four layers** — it lives natively in the Java
client, is preserved through the Rust core (which holds the *logic*), is
unavoidably **flattened at the C ABI** (plain C can't express it), and must be
**restored by each binding**:

| Layer | Role | Kafka Java shape here? |
|---|---|---|
| **Kafka Java client** | The reference API | ✅ **This *is* the shape** — the target |
| **Rust core** | The implementation — **all Kafka logic lives here** | ✅ Preserved (in idiomatic Rust form) |
| **C ABI** (`confluent_kafka.h`) | FFI boundary | ❌ **Flattened away** — no generics / async / exceptions / overloads; only opaque `*_t` handles, `byte* + len`, error out-params, callback function pointers |
| **Language binding** (.NET, Python) | What the user calls — **shape only, no logic** | ✅ **Restored** — the binding's whole job |

So a binding **never re-implements Kafka behavior**; it rebuilds the Java
*shape* on top of the flattened C ABI, reaches the C functions through the
host's FFI, and forwards all real work to the Rust core. Only two things vary
per language:

- the **FFI mechanism** — .NET: P/Invoke; Python: CPython extension
- the **host idioms** used to rebuild — e.g. Java `Future` → .NET `Task`,
  Python `concurrent.futures.Future`.

Target the **Java shape**, *not* the language's existing ecosystem client
(e.g. **not** `confluent-kafka-python`'s `produce()/poll()`) — that's what keeps
all bindings mutually consistent.

**Reconstruction effort scales with distance from C:** C needs none (it *is*
the ABI); rich-OO languages do substantial rebuilding.

**Already real in Python:** `bindings/python/producer.py` restores
`Producer.send(ProducerRecord) → Future[RecordMetadata]`, `RecordMetadata`, and
`KafkaError` on top of the flat `kafka_producer_*` / `kafka_common_KafkaError_*`
C functions — reconstructing their *shape* while all behavior stays in the Rust
core.

---

## 2. Cross-binding conventions

Shared decisions every C-ABI binding follows so the bindings stay mutually
consistent. These are **language-agnostic principles**; each binding's concrete
realization lives in its own `bindings/<lang>/CLAUDE.md`.

1. **API shape target** — Present Apache Kafka's **Java public API** shape. Do
   not adopt the language's existing ecosystem Kafka client shape.
2. **Naming** — Mirror Java class / method / field names, adapting only casing
   to the host's convention. Names stay recognizable across bindings.
3. **Async model** — Map Java's `Future` / blocking calls to the host's nearest
   async idiom, preserving the same semantics.
4. **Resource lifecycle & ownership** — Give every opaque handle deterministic
   cleanup in the host idiom, and honor the ABI's ownership contract (who frees
   what).
5. **Zero-copy at the boundary** — Add no *extra* copies of key/value/header
   bytes when crossing the ABI; pass by reference where the buffer's lifetime
   allows, and copy only when lifetime forces it.
6. **Shape, not logic** — Bindings add **no Kafka behavior**; all logic stays in
   the Rust core.
7. **Review ground truth** — Review a binding against the **C ABI header** and
   the **Kafka Java public API** — not against Java implementation logic.

---

## 3. Structure & layout

Where each rule file lives:

| Tier | File | Holds |
|---|---|---|
| Shared | `bindings/CLAUDE.md` | Cross-binding mental model + conventions (§1–2) |
| Language rulebook | `bindings/<lang>/CLAUDE.md` | The language's concrete realization: bridge/FFI mechanism, naming, async/error/resource mappings |
| Language deep-dives | `bindings/<lang>/.claude/rules/*.md` | Heavy sub-topics referenced by the rulebook (e.g. FFI marshalling & ownership) + Agent role definitions |
| Language agents | `bindings/<lang>/.claude/agents/*.md` | The binding's own Actor/Critic — self-contained personas |
