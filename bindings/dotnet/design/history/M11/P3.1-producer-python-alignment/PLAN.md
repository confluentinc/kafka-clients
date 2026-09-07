# M11 / Phase 3.1 (P3.1) — .NET async producer send path: Python alignment (binding-side accumulator + `send_batch`)

> **Status:** PLAN — **all seven open decisions resolved by the user on 2026-09-07 (§11).**
> No Actor/Critic spawned; the user gates the loop separately.
> **Phase ID:** M11/P3.1 · **Mode:** A (.NET-only; no Rust core / ABI / header change)
> **Branch:** `prashah_dev_producer_python_alignment` (base `f24add9e`)
> **Supersedes:** the Option-A rejection recorded in `design/history/M11/P3-producer-send/PLAN.md` §3.3,
> `design/current/producer-send-completion-approaches.html` (Option A section),
> `design/current/STATUS.md:99`, and `src/Confluent.Kafka/Internal/Interop/NativeMethods.cs:2237-2243`.

---

## 0 · What this phase does, in one paragraph

Replace the async producer's **send submission** side with the Python binding's shape: `Send`
pins the record's buffers and appends it to a **binding-side accumulator**, returns its `Task`
immediately, and a dedicated **batch thread** drains N records into a blittable
`ProducerRecord_t[]`, calls the already-exported `kafka_producer_Producer_send_batch`, unpins,
and hands the resulting futures to the **existing** `SendCompletionPump`. The **completion**
side does not change. The **sync** `Send` path does not change. The public API does not change.

---

## 1 · Parity anchor (mandatory — read this before writing any code)

> **Why this section leads.** M11/P2 shipped invented surface and diverged from its sibling
> precedent despite a green build, green tests, and a clean Critic, and needed a whole follow-up
> phase (P2.1) plus four fixups. The recorded feedback is explicit: *pin a parity anchor up front
> and review against it.* Every design decision below is justified against the anchor, and every
> deliberate departure is listed in §3 so a Critic can tell "deliberate" from "missed".

### 1.1 The anchor

**`bindings/python/_confluentkafka.c`'s producer send path**, specifically:

| Anchor site | What it establishes |
|---|---|
| `:19-27` | `PRODUCER_RECORD_SLOT_THRESHOLD 1000`, `SLOT_CAPACITY 1100`, `MAX_ACCUMULATED_RECORDS 1000` |
| `:30-36` | `ProducerRecordObject` — owns key/value refs **and an owned `topic_owned` malloc** |
| `:360-398` | `BatchNode` (five parallel arrays) + the `Producer` struct's two queues / mutexes / condvars |
| `:480-513` | `Producer_poll_futures_thread` — **already mirrored** by `SendCompletionPump` |
| `:523-655` | `Producer_send_thread` — the middle thread this phase adds |
| **`:533-551`** | **the wait loop; `:535` is the bare 10 ms literal** |
| `:562-621` | take-the-chain, reset backpressure, fire space callbacks, flatten, `send_batch`, immediate-error compaction |
| `:777-834` | `py_Producer_send` — `:825-827` the `>= 1000` signal, `:830` the `full` flag |
| `:843-880` | `py_Producer_on_space_available` |
| `:945-985` | `py_Producer_shutdown` — `:967` signals **and drains** on close |

plus `bindings/python/producer.py:650-702` (`AsyncProducer.send`, `:690-700` awaited
backpressure) and `:631-637` (`_drain`).

**The full analysis is `design/current/python-binding-send-batching.md` (519 lines, verified line
numbers). Do not re-derive it — cite it.** In particular §"The wake-up rule" (the timer is
free-running, so a sub-threshold batch waits 0–10 ms *uniformly*, not 10 ms), §"Backpressure",
and the two Observations.

### 1.2 Terminology (align with the existing doc)

`design/current/producer-send-completion-approaches.html` calls this design **"Option A —
Python-style pull (2 threads)"**, counting the two *background* threads (batch + poll). Use that
convention: **2 threads**, not 3. The caller thread is not counted.

### 1.3 ⚠ Two naming collisions that will otherwise cause false findings

1. **`ffi-marshalling.md` §A7's "Option A/B" ≠ the PLAN's "Option A/B/C/D".**
   §A7 "Option A: pull pump" **is** the shipped PLAN Option C. §A7 "Option B: push callback" is
   PLAN Option B. **PLAN Option A (this phase) is not in §A7's lettering at all** — §A7 refers
   to it only in a parenthetical: *"a send-batching thread is an optional throughput tweak, not
   required"*. So this phase **does not change §A7's completion-model decision**; it implements
   §A7's own parenthetical on top of §A7 Option A.
2. **The HTML doc's "Option D ⬅ CURRENT" header is not true of this branch.** Verified:
   `Producer_send_async` is undeclared (it appears only inside the comment at
   `NativeMethods.cs:2243`), and there is no `Semaphore`/in-flight cap anywhere in
   `NativeProducer.cs` or `SendCompletionPump.cs`. Options B and D live on other branches.
   **The baseline for this phase is Option C, pure.** Treat the HTML doc's *analysis* as input
   and its *status labels* as branch-divergent.

### 1.4 The in-repo pieces to reuse (no reinvention)

- `Internal/SendCompletionPump.cs` — reused for the completion side, with **exactly ONE sanctioned
  change: the `SLOT_CAPACITY` drain cap** (§12.2, and §1.5's carve-out table for the boundary). It is already the
  analog of `Producer_poll_futures_thread` (`ConcurrentQueue`, `ManualResetEventSlim`, batched
  `FutureRecordMetadataGetAll`, `destroy_all`). Its `CloseGate()` / `Stop()` /
  `DrainAndFaultRemaining()` gate machinery is reused unchanged.
- `Internal/SerializedProducerRecord.cs` — the existing bytes carrier (`readonly struct`,
  `string Topic`, `ReadOnlyMemory<byte>? Key/Value`). The accumulator stores these.
- `Internal/DeliveryRegistration.cs` + `IDeliveryCallback` — the M14/P1 firing site and its
  four recorded residuals. Preserved; see §6.
- `Internal/Interop/ProducerSendMarshal.cs` — **left in place and still used by the sync path.**
  The batch marshal is a new sibling, not a replacement.
- `NativeProducer`'s `_closed` latch / `TryBeginClose()` / `EnsurePump()` / `PumpToStop()` shape
  — extended, not restructured.

### 1.5 NOT-adding list (the Critic flags any of these appearing)

- Any change to the **sync** `Send` / `Flush` / `Close` path (`NativeProducer.cs:579`, `:665`).
- Any change to the **public** API: `IAsyncProducer`, `AsyncKafkaProducer`, `AsyncMockProducer`,
  `ProducerRecord<K,V>`, `RecordMetadata`, `IDeliveryCallback`, mock helpers. Internal-only.
- `Producer_send_async` / any per-send Cdecl callback (that is Option B).
- A `SemaphoreSlim` **in-flight** cap on *acked-but-unresolved* sends (that is Option D — see
  §3.7; it is orthogonal and out of scope).
- `Headers` on the record (`ProducerRecord_t` has no headers field).
- A per-record **copy** of key/value into a pooled buffer (CLAUDE.md §12; user-directed).
- A second completion pump, or moving completion off `SendCompletionPump`.

**⚠ Amended (user, 2026-09-07) — one narrow carve-out in `SendCompletionPump`.** §1.4 originally
said the pump is reused **as-is** and this list forbade changing it. The pump is now in scope for
**one change only: capping its drain at `SLOT_CAPACITY`** (§12.2), together with the loop change
that cap *requires* and the doc corrections it forces. Stated as a boundary a Critic can apply
mechanically:

| In scope (sanctioned) | Out of scope (still forbidden) |
|---|---|
| Cap `DrainAll` at `SLOT_CAPACITY` | Capping `DrainAndFaultRemaining` — §12.2 explains why this would be a defect |
| The `RunLoop` inner drain loop that the cap requires (§12.2 "the hang hazard") | Any other `RunLoop` restructuring: the `Wait`/`Reset` ordering, the `_stopping` break, the fault-and-**continue** `catch` |
| Reusing three `IntPtr[SLOT_CAPACITY]` arrays in `ProcessBatch` (§12.3) | Changing `ProcessBatch`'s per-index completion logic, its handle-freeing paths, or its `finally` sweep |
| Rewriting the stale `:59-62` backpressure paragraph (§12.4) | Changing `Stop` / `CloseGate` / `Enqueue` / `_stopLock` semantics, or the queue type |
| Narrowing the residual-3(b) wording that §12.3 makes stale | Adding, removing, or re-scoping any *other* recorded residual |

Anything touching the pump beyond that table is an unsanctioned change, regardless of merit.

---

## 2 · Superseding the recorded rejection (do this first, in code and in docs)

Option A was **analysed and rejected** on this repo's own record. The plan does not re-argue it —
the user has been shown the trade-offs (latency floor, pin pressure, "no upside over C on .NET")
and has reaffirmed the design — but the record must be **explicitly superseded**, not silently
contradicted, or every Critic round will re-file the rejection as a finding.

### 2.1 The four places that record the rejection

| Location | Current text (abridged) |
|---|---|
| `NativeMethods.cs:2237-2243` | "No `ProducerRecord_t` mirror struct (that is send_batch / Option A), no per-send callback…" |
| `design/current/STATUS.md:99` | "No `ProducerRecord_t` mirror struct (that was Option A / `send_batch`), no managed accumulator bound." |
| `design/history/M11/P3-producer-send/PLAN.md` §3 / §3.3 | the A/B/C decision + "Preference: Option C — rationale" |
| `design/current/producer-send-completion-approaches.html` | Option A section: pros/cons, ASCII pin diagram, "Status: rejected" |

### 2.2 What is genuinely new since that decision (the honest case for reopening)

The rejection's headline con was *"No upside over C on .NET — Python batches in C to amortize
the GIL; .NET has no GIL, so the extra threads buy nothing."* That remains true **for
throughput**. But one upside was recorded *elsewhere* and never credited in the Option A
section:

> `project_dotnet_producer_option_c_backpressure_close_limitation` (agent memory, 2026-08-14),
> and the same limitation in STATUS's accepted residuals: under Option C, backpressure is the
> core's `buffer.memory`, so an inline `Producer_send` **blocks the caller thread** up to
> `max.block.ms` inside the coarse `Mutex<ProducerKind>` — and a **concurrent `close` cannot
> wake it**, because close needs the same mutex. Java diverges from us twice here (Java's close
> doesn't wait, and Java's close *wakes* the stuck send). That memory names **"switch to Option
> A"** as escape hatch #1, explicitly *"binding-side, Mode A, internal-only, non-breaking"*.

So the accurate framing is: **Option A converts the caller's block from a native block inside
the FFI mutex into a managed, cancellable wait** — which is what makes it closer to Java on
`send`, and is a concrete fix to a residual this repo already accepted. §3.6 states precisely
how much of that limitation it fixes (not all of it).

### 2.3 Deliverable

Rewrite (do **not** delete) all four records to state the split: *sync path = Option C; async
path = Option A; completion side = §A7 pull, unchanged; superseded by M11/P3.1 on user
direction, with the §3 deviation list.* Reuse the HTML doc's existing hazard analysis rather
than re-deriving it — and correct its two factual errors (§3.9).

---

## 3 · Deliberate deviations from the anchor (each with a rationale)

### 3.1 The sync path stays inline (user-directed, constraint 2)

Python has no sync/async split at its C layer — both go through one accumulator. But the
*shapes* differ: Python's sync `send` returns a `Future` (non-blocking), whereas .NET's sync
`Send` returns a fully-materialized `RecordMetadata` (it blocks on `FutureRecordMetadata_get`).
Routing a blocking send through a 0–10 ms accumulator window would add that window to **every**
sync send's latency — against a measured baseline of p50 ≈ 7 ms, i.e. a potential doubling.
**Decision:** sync stays exactly as it is (`NativeProducer.cs:579`, `ProducerSendMarshal.Send`,
call-scoped `fixed` pins). This is a deliberate, user-directed asymmetry.

### 3.2 The anchor's constants are kept — by name and by value — but made tunable

**Values and names are Python's (D1, D2, D7).** `SLOT_THRESHOLD = 1000`,
`SLOT_CAPACITY = 1100`, `MAX_ACCUMULATED_RECORDS = 1000`, and a 10 ms window — the same
identifiers as `_confluentkafka.c:19-27` and `:535`, so a reader can diff the two bindings
line-for-line. This is deliberate parity, not a default chosen on .NET grounds.

**The one thing that changes is that they stop being invisible.** The anchor's 10 ms is a **bare
literal** at `_confluentkafka.c:535` — no `#define`, no config key, not surfaced in any docstring
(Observation 2). That undocumented-ness is what §3.3 rejects; the *magnitude* is kept.

**Decision — named constants, each overridable by an interim env var read once at
construction**, following the exact precedent of Option D's
`CONFLUENT_KAFKA_PRODUCER_MAX_INFLIGHT_SENDS` (*"read once at construction; not a Kafka
config-dict key"*). There is no `ProducerConfig` type in this binding — config is a
`KeyValuePair<string,string>` dict consumed by the core — so inventing a config-dict key the
core does not know would be new public surface. Env vars avoid that.

| Constant | Python name / site | Default | Env override |
|---|---|---|---|
| drain threshold (early-wake trigger) | `PRODUCER_RECORD_SLOT_THRESHOLD` `:19` | **1000** | `CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD` |
| node capacity | `PRODUCER_RECORD_SLOT_CAPACITY` `:20` | **1100** (threshold + 100) | — (derived) |
| accumulated bound (backpressure) | `PRODUCER_MAX_ACCUMULATED_RECORDS` `:27` | **1000** (= threshold) | `CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED` |
| linger window | *(bare literal)* `:535` | **10 ms** | `CONFLUENT_KAFKA_PRODUCER_BATCH_WINDOW_MS` |
| **per-`send_batch` chunk** | *(no Python **name** — its effective value is `SLOT_CAPACITY`; §3.4)* | **1100** (= `SLOT_CAPACITY`, i.e. one call per node, as in the anchor) | `CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK` |

The fifth row is the only net-new **constant name**; its **default value is Python's**. Python has
no chunk identifier, but its de-facto per-call maximum is one `BatchNode`, and a node fills to
exactly `SLOT_CAPACITY` — a new node is allocated only at `count == PRODUCER_RECORD_SLOT_CAPACITY`
(`:806`), the flat array handed to `send_batch` is sized `SLOT_CAPACITY` (`:585`), and one call is
issued per node (`:593`). So **1100 is the Python-faithful value; 1000 is the threshold, which is
a different bound** (wake trigger + backpressure).

**Its tunability is purely a D3 escape hatch and does not change default behavior.** At the
default the chunk equals node capacity, so it can never split a node — see §3.4. Lowering it
below `SLOT_CAPACITY` is the **only** way to make the chunk observable at all, and doing so is a
deliberate divergence from the anchor that an operator opts into.

### 3.3 The 10 ms window is kept — its cost is accepted, and it is now tunable

The anchor's timer is **free-running** (`:535` sets the deadline at the top of the send thread's
loop from its own clock, unrelated to record arrival), so a sub-threshold batch waits
**0–10 ms uniformly** — mean ~5 ms, and the same code with the same record count varies by two
orders of magnitude run to run (`design/current/python-binding-send-batching.md`, "The timer is
free-running"). In Python that cost is bought back by amortizing the GIL and the boundary
crossing; in .NET, where a P/Invoke is ~5 ns, there is no equivalent purchase.

**Decision (D2): keep 10 ms, and keep the free-running-timer shape.** The 0–10 ms stage-1 delay
is an **accepted cost** of Python parity, mitigated three ways: it is a named constant, it is
env-tunable per §3.2, and it will be **measured** under the deferred perf work (§8.2).

Two things follow, and both must be written at the implementation site so a future reader does
not mistake the accepted cost for an oversight:

- **The delay is uniform over 0–10 ms, not a fixed 10 ms.** Any test asserting stage-1 timing
  must assert a *bound*, never an expected value (§8.1 tests 1–2).
- **A first-record-starts-the-timer variant is explicitly out of scope for this phase.** It would
  change the *mechanism* as well as the magnitude, and doing both at once would make the deferred
  perf result uninterpretable. It is the natural first thing to try if that result is bad.

### 3.4 One `send_batch` per chunk, chunks never spanning a node — and the chunk is a decoupled, tunable constant

**Decision (D1): chunk at `SLOT_CAPACITY` = 1100 — Python's effective per-call maximum.** The
anchor issues one `send_batch` per `BatchNode`, walking the chain (`:593`; a 2447-record chain is
3 calls). Never one call for the whole drained chain.

**The chunk is its own named constant, independently tunable from node capacity** (§3.2, row 5).
The anchor couples the two only because its `BatchNode` arrays are fixed-size allocations
(`_confluentkafka.c:806`) — the chunk *is* the array. In .NET nothing forces that coupling, and
decoupling costs nothing: it makes the deferred perf result (§8.2) **actionable as a one-line
change** rather than a redesign, since chunk size is the single knob that trades batching
efficiency against the mutex hold below.

#### The exact rule — which reduces to the anchor's behavior at the defaults

> **One `send_batch` per chunk, and a chunk never spans two nodes.** Walk the chain node by node;
> within a node, emit `ceil(count / chunk)` calls.

**At the default the formula reduces to exactly one call per node — identical to the anchor.** A
node can never hold more than `SLOT_CAPACITY` records (`:806`), and the chunk *is* `SLOT_CAPACITY`,
so `ceil(count / chunk)` is always 1. **There is no divergence from the anchor at the defaults.**

Keep the formula anyway: it is what makes a **lowered** chunk safe, and lowering the chunk is the
sole reason the constant exists (the D3 escape hatch, §3.2). It only ever splits a node if an
operator sets `CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK` below `SLOT_CAPACITY`. State the formula at
the site so a future reader does not "optimize away" a branch that looks dead at the default —
it is dead at the default *by construction*, and load-bearing under an override.

#### The accepted cost: one long mutex acquisition instead of many short ones

`send_batch_inner` (`src/ffi/producer.rs:1483-1571`) acquires `handle.kind.lock()` **once, for
the whole batch** (`:1500`) and then loops `producer_send(&guard, record)` per record — where
`producer_send` (`:347-380`) is `rt.block_on(producer.send(record, None))`. So **one
`send_batch(N)` call holds the coarse producer mutex across N sequential `block_on` sends.**

This is a **recorded, accepted cost**, not a rationale for a smaller chunk. Stated precisely:

- **The bound is up to `SLOT_CAPACITY` = 1100 records per acquisition.** Not 1000: the chunk is
  the per-call bound and it equals node capacity (§3.2). That is 10% above the 1000 figure an
  earlier draft of this plan quoted, on a cost that was already accepted — and it is what buys
  **exact** parity with the anchor's one-call-per-node behavior, so the 10% is the price of
  removing a divergence rather than a new cost being introduced.
- **Total mutex-held time is roughly unchanged from Option C.** ~1100 short acquisitions become
  one long one. So this is **not a throughput regression** — it is a **fairness / tail-latency**
  change in who waits, and for how long at a stretch.
- **The contention is *between surfaces*, and an async-only application sees none of it.** With
  only the async path in use, the batch thread is the sole holder of that mutex on the send path,
  so there is nothing to contend with. The blast radius is exactly: (a) **mixed sync + async**
  use of one producer, where a sync `Send` can now queue behind up to a full chunk; and
  (b) `Flush` / `Close` / `Metrics` / `PartitionsFor` on an async producer, which take the same
  mutex and can now be parked for a chunk rather than a record.
- **Worst case is bounded but long:** if record #1 blocks on a full `buffer.memory` for
  `max.block.ms` (default 60 s), records #2..N wait *and* the mutex is held for that whole time.
  This is the same limitation §3.6 discusses, with the window widened by the chunk.

**The chunking still bounds the hold, so it must not be "simplified".** In the anchor, per-node
chunking is an artifact of fixed-size arrays; here it is *also* the mechanism that caps how long
the mutex is held in one stretch. A future refactor that collapses the per-node loop into a
single `send_batch` over one flattened `List<T>` would remove that cap silently. **State this
reason in a comment at the call site.**

The only complete fix is finer-grained locking in `src/ffi/producer.rs` (**Mode B**, out of
scope) — see §3.6.

### 3.5 `Flush` MUST drain the accumulator (do NOT copy the anchor's gap)

Observation 1: the anchor's `flush()` never signals its send thread — `record_batches_new_record_cnd`
has exactly three signal sites (`:826` at 1000 records, `:967` on close, `:895` test-only) and
**none is a flush entry point** (`py_Producer_flush :1119`, `py_Producer_flush_async :1238` both
go straight to the Rust producer). So Python's flush can return while records are still buffered
in the binding, which arguably violates Java's contract that `flush()` blocks until every
*previously sent* record completes — from the caller's view those records **were** sent, because
`send()` returned.

**Decision:** in .NET, **both** `Flush` (sync-on-the-async-producer: `FlushWithCallback`) and the
teardown flush drain the accumulator **before** calling into the core. This is a deliberate
divergence *toward* Java. It gets its own slice (S6) and its own test. The fix shape is the
anchor's own `py_Producer_shutdown` (`:967`): signal the batch thread and wait for the
accumulator to reach empty, then flush the core.

### 3.6 It only partially fixes the recorded close/backpressure limitation — say so

Do **not** claim Option A fixes the residual outright.

- **Fixed:** the *caller* no longer blocks inside the coarse FFI mutex. `Send` appends to the
  accumulator and returns; backpressure is an `await` on a managed primitive that teardown can
  cancel. That is the Python behavior and it is closer to Java.
- **Not fixed, and the window grows:** the **batch thread** still calls `send_batch`, which takes
  the same mutex and can block on `buffer.memory` for up to `max.block.ms`. A concurrent `Close`
  is still parked behind it — and now behind up to N records instead of one (§3.4). Python has
  the identical structure and the identical parking.

The only complete fix remains escape hatch #2 from that memory: finer-grained locking in
`src/ffi/producer.rs` — **Mode B, out of scope.** Flag it as a follow-up.

### 3.7 The accumulator bound is **not** a substitute for Option D's in-flight cap

Easy to get wrong, and it drives the perf expectation. The two bounds sit on **opposite sides of
the core**:

- **Option A's accumulator bound** (this phase) bounds records **queued in the binding, not yet
  sent** — *upstream* of the core.
- **Option D's `SemaphoreSlim`** bounds records **sent and not yet acked** — *downstream*.

Option C's measured latency problem (async p50 ≈ 96 ms) is caused by the **deep downstream**
pipeline: the core's 32 MB `buffer.memory` ≈ 32k in-flight records. Option A does nothing about
that. So the expected async latency is **Option C's latency plus 0–10 ms**, not an improvement.
Option D is orthogonal and composable with A (a future phase could add it); this phase must not
smuggle it in (§1.5).

### 3.8 Teardown becomes a three-party handshake (highest-risk item — own slice)

Today teardown is two-party. Verified shape: every flavor is
`latch → pump.CloseGate() → flush → pump.Stop()/join → Producer_close → _handle.Dispose()`, via
`TryBeginClose()` (`:1419`) → `StopPump()` (`:931`) / `StopPumpAsync()` (`:995`) from `Close`
(`:1216`), `Dispose` (`:1261`), `DisposeAsync` (`:1299`), `CloseWithCallback` (`:1165`).

The accumulator inserts a third party that holds records which have **not yet reached the core
at all** — so they are not futures, and the pump knows nothing about them. **The required
ordering is:**

```
1. TryBeginClose()                    win the latch (unchanged)
2. cancel the backpressure gate       wake any Send awaiting space, so close can't hang
3. close the accumulator to new appends
4. signal the batch thread + WAIT for the accumulator to reach empty
     -> its send_batch calls run -> futures are enqueued to the pump
     (the pump's gate is STILL OPEN here — this is the load-bearing bit)
5. pump.CloseGate()                   only now
6. flush (sync ProducerFlush / await FlushInternal)   unchanged
7. pump.Stop()  -> join              unchanged
8. Producer_close -> _handle.Dispose()                unchanged
```

Steps 2–4 are new and **must precede** step 5. Getting 4 and 5 the wrong way round is silent
data loss dressed as a residual: the batch thread's futures would arrive at a closed gate and be
faulted in place by `SendCompletionPump.Enqueue`'s `_stopped` branch (`:156-158`) — which is
**recorded residual 1** and fires **no delivery callback**. A correct implementation must not
route normal teardown through a residual path.

### 3.9 Two corrections to the prior HTML analysis

Both are in its Option A row and both matter for the implementation:

1. **"Pin mechanism: `GCHandle.Alloc(Pinned)`" is wrong for this binding.** The key/value are
   `ReadOnlyMemory<byte>?` (`SerializedProducerRecord`), and `GCHandle.Alloc` cannot pin a
   `ReadOnlyMemory<byte>` — it pins objects. The correct primitive is
   **`ReadOnlyMemory<byte>.Pin()` → `MemoryHandle`**, which handles every backing store (array,
   string, native memory, custom `MemoryManager`) and is available on **all three TFMs**
   (`netstandard2.0` gets it from the already-referenced `System.Memory` 4.5.5). See §4.2.
2. **The pin window is shorter than the doc's diagram implies — it ends when `send_batch`
   returns, not "at the copy".** Verified: `send_batch_inner` → `producer_send` →
   `rt.block_on(producer.send(record, None))`, so each record's bytes are serialized into the
   batch buffer **synchronously inside the `send_batch` call**. The topic is likewise copied
   (`to_string_lossy().into_owned()`, `:1515`). So the deferred pin must span
   `Send → …accumulator… → send_batch returns` — and **not** the returned `Task`. This is the
   same "borrow ends when the call returns" fact §A4 rests on, merely applied to `send_batch`
   instead of `send`. It is what keeps this design inside §A4's existing carve-out (§5.1).

---

## 4 · Design

### 4.1 THREE buffers need lifetime coverage per record, not two

The ABI struct (`src/ffi/producer.rs:277-292`, `#[repr(C)]`, all blittable):

```rust
pub struct kafka_producer_ProducerRecord_t {
    pub topic: *const c_char,   // NUL-terminated UTF-8
    pub partition: i32,
    pub timestamp: i64,
    pub key: *const u8,
    pub key_len: i32,
    pub value: *const u8,
    pub value_len: i32,
}
```

`topic` is the one that gets missed. Today it is a call-scoped `Utf8Marshal.Pin(topic)`
(`ProducerSendMarshal.cs:64`) and there is nothing to think about; deferred, a stack/call-scoped
topic pointer is a **use-after-free**. The anchor covers this with an owned `topic_owned` malloc
per record object (`_confluentkafka.c:30-36`).

**Decision — intern per topic, don't pin per record.** One permanently-pinned NUL-terminated
UTF-8 buffer **per distinct topic name**, cached on the `NativeProducer` in a
`ConcurrentDictionary<string, IntPtr>`, allocated with `GCHandle.Alloc(bytes, Pinned)` and freed
at producer teardown. Rationale: topics are few and long-lived, so this is **O(distinct topics)
permanent pins instead of O(records) transient pins** — it drops the per-record pin count from
3 to ≤2 and removes a whole class of fragmentation the prior analysis charged against Option A.
(Uniform `GCHandle.Alloc(Pinned)` on all TFMs deliberately: the Pinned Object Heap
(`GC.AllocateArray(pinned: true)`) is net5.0+ and would need a `#if` for the `netstandard2.0`
leg, for no benefit on a handful of permanent buffers.)

**Decision (D5, user, 2026-09-07): bound the cache, with a stated eviction behavior.** An
application sending to unbounded distinct topics would otherwise grow it without limit. Cap it at
**1024 entries**; beyond the cap, fall back to a **per-record pinned topic buffer** (3 pins for
those records instead of 2) rather than evicting — evicting a pinned buffer that an in-flight
accumulator node still points at would be a use-after-free, so the cache is **insert-only for the
producer's lifetime and never evicts**. Freed as a whole at producer teardown. State the cap, the
fallback, and the no-eviction rule at the site.

### 4.2 The empty-key/value sentinel cannot be a stack byte any more

`ProducerSendMarshal.cs:69-72` uses `byte emptySentinel = 0;` — a **stack** address — because
`fixed` over an empty span yields `null`, and the core rejects `(null, len >= 0)`. Deferred,
that stack address is dead by the time the batch thread reads it. This will "work" most of the
time and corrupt rarely, which is the worst failure mode.

**Decision:** one **process-wide statically-pinned 1-byte sentinel** (a `static readonly`
1-element array pinned once via `GCHandle.Alloc(Pinned)`), used for every empty-but-present
key/value. Sentinel semantics are otherwise unchanged (§A4): absent → `IntPtr.Zero` + `-1`;
empty → non-null + `0`; present → pointer + length.

### 4.3 Types

```
Internal/Interop/ProducerRecordNative.cs     [StructLayout(LayoutKind.Sequential)] mirror of the ABI struct
Internal/Interop/ProducerSendBatchMarshal.cs pin/unpin + fill the native array + call SendBatch
                                             + immediate-error compaction  (the only new `unsafe`)
Internal/SendAccumulator.cs                  the node chain, its lock, the counter, the thresholds,
                                             the backpressure gate, the batch Thread
Internal/PinnedTopicCache.cs                 §4.1 interning
```

`unsafe` stays quarantined in `Internal/Interop/` (CLAUDE.md §2). `SendAccumulator` holds
`MemoryHandle` values but needs no `unsafe`.

Accumulator node, mirroring the anchor's five parallel arrays (`:361-369`):

```
SerializedProducerRecord[]     records          (already a readonly struct)
MemoryHandle[]                 keyPins          (default = not pinned)
MemoryHandle[]                 valuePins
TaskCompletionSource<...>[]    completions
DeliveryRegistration?[]        deliveries
IntPtr[]                       futures          filled by send_batch
IntPtr[]                       errors           filled by send_batch
int                            count
Node?                          next
```

### 4.4 Pin lifecycle — the exact contract

```
Send (caller thread)
  ├─ acquire backpressure permit  (fast path: non-blocking TryWait)
  ├─ pin:  key?.Pin()  value?.Pin()      -> 0..2 MemoryHandle
  ├─ topic -> interned pinned IntPtr     (§4.1, no per-record pin)
  ├─ append to the tail node under the accumulator lock
  ├─ if node.count >= threshold -> signal the batch thread   (anchor :825-827)
  └─ return completion.Task

Batch thread
  ├─ wait: signal OR window elapsed      (anchor :533-551)
  ├─ take the whole chain; reset the counter; release backpressure waiters (anchor :562-579)
  └─ per node:
       ├─ fill ProducerRecordNative[] from records + pin pointers
       ├─ SendBatch(handle, arr, n, futures, errors)
       ├─ **UNPIN every MemoryHandle in this node — in a `finally`**
       ├─ compact: complete/fault the immediate-error indices here (anchor :600-621)
       └─ enqueue the surviving (future, tcs, delivery) triples to SendCompletionPump
```

**Invariants the Critic must check:**
- Every `MemoryHandle` obtained in `Send` is disposed **exactly once**, on **every** path —
  including: the append throwing, the accumulator being closed by teardown, `SendBatch` throwing,
  a node abandoned by teardown, and the immediate-error path.
- Unpin happens **after** `SendBatch` returns and **before** the futures reach the pump. Never
  in the pump. Never across the returned `Task`.
- The pin must not be taken **before** the backpressure permit, or a blocked sender holds pins
  while waiting — turning the bound on records into an unbounded pin window.

### 4.5 Immediate-error path

`send_batch_inner` writes `out_futures[i]` / `out_errors[i]` per record and returns the success
count. Mirror the anchor (`:600-621`): on the batch thread, for each index with a non-null error,
fault that record's TCS, fire its delivery callback, free the error handle, and **compact** the
arrays so only successful `(future, tcs, delivery)` triples reach the pump. Null-topic and
null-with-non-negative-length are core-side `InvalidRequest` errors (see below) —
they arrive through this path, not as a thrown exception. The three core-side sites are
`src/ffi/producer.rs:1510` (null topic), `:1521` (null key with `key_len >= 0`), and `:1534`
(null value with `value_len >= 0`).

### 4.6 Backpressure

The anchor returns a `full` boolean from `send` (`:830`) when `accumulated_records >= 1000`, and
Python then waits (`producer.py:690-700` async); the drain resets the counter (`:573`) and fires
the space callbacks (`:579`). The check-and-register runs under the **same mutex** the drain
holds (`:843-880`), so there is no lost-wakeup window, and if the drain already ran it returns
immediately without waiting (`:855-859`).

**Carry the anchor's own rationale across — it cites Java.** The comment on
`PRODUCER_MAX_ACCUMULATED_RECORDS` (`:21-26`) reads: *"once this many records are accumulated but
not yet taken by the send task, the producer is 'full' and further enqueuing should wait until the
send task drains a batch. **One complete batch beyond the one being filled — mirrors Java's
`send()` blocking once `buffer.memory` is full, applied here at batch granularity in front of the
Rust accumulator.**"* That is the Java-faithfulness argument for having a stage-1 bound at all,
and for it equalling the threshold rather than being an independent number. Reproduce it at the
.NET site; it is the answer to "why is there a second bound in front of the core's own?".

**.NET:** a `SemaphoreSlim(bound, bound)`. `Send` takes a permit — `Wait(0)` fast path (no
allocation, preserving the DoD §10 budget), else `WaitAsync(token)`; the batch thread releases
the drained count. **`Send` stays `async`-free on the fast path**: it must remain a plain method
returning `Task` so a serializer throw stays synchronous (the existing
`AsyncKafkaProducer.SendValidated` is deliberately not `async` for exactly this reason, and the
Critic memory records that synchronous `SerializationException` is Java-faithful). Only the slow
path yields.

The gate is cancelled by teardown step 2 (§3.8), so `Close` can never hang behind a blocked
sender — the one place this design is strictly better than Option C.

### 4.7 D6 — the buffer-mutation window is documented on the ASYNC surface ONLY

**Decision (D6, user, 2026-09-07): accept the behavior change and document it.** Under Option C
the core copied key/value synchronously inside `Producer_send`, so a caller mutating its buffer
after `Send` returned could not affect the produced record. Under Option A the async path defers
the send, so a mutation between `Send` returning and the drain **is** visible on the wire. This
is inherent to deferring a zero-copy send; the only alternative is the per-record copy
CLAUDE.md §12 forbids. It matches the anchor, where Python's `send()` likewise borrows the
buffer until the drain.

**⚠ Scope: the async surface only — `IAsyncProducer.Send` / `AsyncKafkaProducer` /
`AsyncMockProducer`. Documenting it on the sync `Send` would be a defect, not extra caution.**

The sync path is unchanged (§3.1) and still calls `Producer_send`, where the core copies
key/value **synchronously during the call** — the verified fact §A4 rests on
(`src/ffi/producer.rs:262-281`; `producer_send` at `:347-380` is
`rt.block_on(producer.send(record, None))`). **There is no mutation window on the sync surface at
all.** A doc comment asserting one would state a constraint that does not exist, tell sync users
to defend against an impossible race, and — worst — imply the sync path was also deferred, which
is exactly the confusion §3.1 exists to prevent. Treat a mutation-lifetime note appearing on
`IProducer.Send` / `KafkaProducer` / `MockProducer` as a review finding.

**Deliverables:**

- A remarks block on `IAsyncProducer`'s two `Send` overloads: the key/value buffers must not be
  mutated after `Send` returns; the binding borrows them until the record is handed to the core.
- The same note on `AsyncKafkaProducer` / `AsyncMockProducer` where they restate the contract.
- **Nothing on `IProducer` / `KafkaProducer` / `MockProducer`.**
- §8.1 test 9 asserts the window, and **targets the async surface only** — there is no
  corresponding sync test to write, because the sync behavior is unchanged and is already covered
  by the existing §A4 mutation-after-send test (which must keep passing untouched, §8.1 test 22).

---

## 5 · Rules: conflicts, provenance, and required amendments

Per the standing instruction: where a documented rule appears to block this user-directed design,
surface the conflict rather than designing around it. Three rule sites are in play. **None is a
blocker** — two already contain the carve-out, and the third needs a factual amendment.

### 5.1 `ffi-marshalling.md` §A4 — already carves this out; needs an amendment for the window

§A4's Rule **already sanctions batch pinning**, verbatim:

> "Prefer a `fixed` block (stack-scoped, no allocation) for a single send; use
> `GCHandle.Alloc(Pinned)` + `finally Free()` where `fixed` doesn't fit (**the N buffers of
> `_send_batch`, all pinned for the whole call**). Unpin right after the call — never hold a pin
> across the returned `Task`."

and it **explicitly anticipates this exact phase**:

> "This call-scoped rule depends on §A7's inline-send decision; **a deferred-send design (a
> background send thread, as in the Python binding) would have to hold the buffer until the
> deferred send runs.**"

So §A4 is not violated: the pin still ends when the native call returns and still never spans the
`Task`. **Amendment required (deliverable):** state the deferred-send window explicitly — pinned
from `Send` until `send_batch` returns — and correct the primitive from `GCHandle.Alloc(Pinned)`
to `ReadOnlyMemory<byte>.Pin()`/`MemoryHandle` for key/value (§3.9), while keeping
`GCHandle.Alloc(Pinned)` for the interned topic/sentinel buffers (§4.1/§4.2). Also add the
static-sentinel rule — §A4's current stack-sentinel guidance is correct **only** for the
call-scoped case and is a use-after-free if inherited here.

### 5.2 `ffi-marshalling.md` §A1 — a real conflict; needs amendment

§A1's Decision and Rule cap the managed side at one background thread:

> "the .NET side adds at most **one** completion pump (§A7), never a per-send thread or a poll loop."
> ".NET side: caller thread(s) + **exactly one** completion pump (§A7); no per-send threads."

This phase adds a **second** background thread. Strictly read, the batch thread is neither a
"completion pump" nor a "per-send thread" nor a "poll loop" — the prohibitions all still hold
(it is one thread for all sends, and it does not poll). But "exactly one" will read as a cap, and
§A1's own thread diagram would be stale. **Amendment required (deliverable):** update the
diagram and the Rule to "at most **two** — one completion pump (§A7) and, on the async path
only, one send-batch thread (§A7's send-batching tweak)"; keep the "never a per-send thread, never
a poll loop" prohibitions verbatim. Note §A1's *Tests required* already contains an
`(Option A only)` line — *"a long-blocked pump doesn't stop new sends being enqueued"* — which
refers to §A7's Option A and is already satisfied.

### 5.3 `ffi-marshalling.md` §A7 — not touched

This phase does **not** change the completion model. §A7 Option A (pull pump) stays, and §A7
itself calls the send-batching thread *"an optional throughput tweak, not required"*. Add one
sentence recording that the tweak has now been taken on the async path, and why (§2.2). Do not
restructure §A7 and do not reopen pull-vs-push.

### 5.4 Provenance

None of the three sites is agent-authored boilerplate blocking parity — all carry a stated *Why*
and a cited contract, which per the standing guidance makes them the binding class. They are
being **amended for a design the rules themselves anticipated**, not overridden. **Do not edit
root `CLAUDE.md`.**

---

## 6 · The delivery-callback contract and the new orphan class

### 6.1 `IDeliveryCallback` exactly-once must survive

There is no latch on `DeliveryRegistration` — the exactly-once guarantee is **positional**:
exactly one `Fire` is reachable per record per path, invoked unconditionally, and `Fire` itself
is the total no-throw boundary (`DeliveryRegistration.cs:123-144`). The five existing call sites
are `SendCompletionPump.cs:396/:407/:419` (async) and `NativeProducer.cs:613/:636/:645` (sync).

This phase adds **one** new firing site: the immediate-error compaction on the batch thread
(§4.5). It must fire exactly once for those indices, and those indices must not also reach the
pump. Everything else routes through the unchanged pump sites.

### 6.2 A fifth residual is likely — decide it deliberately, don't discover it

The four recorded residuals are enumerated verbatim in `IDeliveryCallback.cs:141-232`. Note this
phase moves that enumeration in **both** directions, so treat it as two separate edits: §12.3
**narrows** residual 3(b) (preallocating the marshalling arrays removes one of its two triggers),
while the paragraph below considers whether to **add** a fifth. Do not conflate them.

This phase introduces a new way to lose a notification: **a record accepted into the accumulator
whose node is abandoned before `send_batch`** (teardown, or a throw between append and the native
call).
Unlike the existing four, the core has **not** accepted such a record, so faulting it is
*correct and complete* — the TCS faults, the delivery callback fires, no duplicate is possible.

**Therefore the target is zero new residuals**, achieved by the §3.8 ordering (drain before
`CloseGate`) plus faulting any abandoned node. If the Actor cannot close a path, it becomes a
**documented fifth residual** added to the `IDeliveryCallback` enumeration with the same rigor as
the other four — not a silent gap. **State which outcome was reached in the phase self-review.**

### 6.3 Re-run the pump-orphan analysis against the accumulator

The `dotnet-critic` memory `feedback_producer_pump_orphan_and_send_fp_calibration` records that
`SendCompletionPump.Stop()` does an **unbounded** `_thread.Join()` on a thread blocked inside the
uninterruptible `FutureRecordMetadataGetAll`, and that the flush-before-join fix rests on the
premise *"flush resolves every pending send"* — a premise `AsyncMockProducer.Clear()` breaks
(`MockProducer::clear` drops completions without completing them → all three teardown flavors
hang forever). That finding was missed by the phase reviews and 136 green tests.

**Required in S5:** re-run that enumeration with the accumulator in place, and check the two new
premises the §3.8 ordering introduces — *"the accumulator always reaches empty"* (what if the
batch thread is blocked in `send_batch` on a full buffer, or dead from an unhandled throw?) and
*"every drained node's futures reach the pump before `CloseGate`"*. Both wait steps need a
**bounded** wait with a defined outcome on expiry, not an unbounded one. The memory's repro
recipe (a 10 s `Thread.Join(timeout)` around teardown in a scratch console app that
`ProjectReference`s the binding csproj) applies directly.

---

## 7 · Slicing

Ordered to keep the tree correct at every commit and to isolate the two L items. Each slice is a
separately reviewable unit; the Critic runs after each (per `agent-roles.md`).

| S | Slice | Size | Why here |
|---|---|---|---|
| **S0** | **Supersede the decision records** (§2.3) — `NativeMethods.cs:2237-2243`, `STATUS.md:99`, the HTML doc's Option A section + its two corrections, a pointer in P3's PLAN §3.3. Docs only, no behavior. | S | **First.** A Critic reading the current record mid-phase would correctly flag the entire design. |
| **S1** | ABI wiring, pins still call-scoped: `ProducerRecordNative`, the `ProducerSendBatch` P/Invoke, `ProducerSendBatchMarshal`, immediate-error compaction. Route `SendViaPump` through `send_batch` with **n=1**, pinned inside the call. | M | Behavior-identical to today; proves the mirror struct, the sentinels, and the per-record error semantics with **zero** new lifetime risk. This is what de-risks the L pinning item. |
| **S2** | The deferred-pin machinery: `PinnedTopicCache` (§4.1), the static sentinel (§4.2), `MemoryHandle` bookkeeping + the exactly-once unpin contract (§4.4). Still no accumulator — `Send` pins, calls `send_batch(1)`, unpins. | **L** | Changes only *when* unpin happens. Isolates the memory-safety core from the concurrency core. |
| **S3** | `SendAccumulator`: node chain, lock, counter, thresholds, the batch `Thread` + the free-running wait loop (§3.3), one `send_batch` per node (§3.4). Teardown here is **correct-but-minimal** (drain synchronously before `CloseGate`). | **L** | The concurrency core. Minimal-correct teardown so no commit is knowingly broken; S5 hardens it. |
| **S4** | Backpressure: `SemaphoreSlim`, `Wait(0)` fast path, drain-side release, teardown cancellation (§4.6). | S | Small once S3 exists. |
| **S5** | **Teardown hardening** — the full §3.8 ordering, bounded waits with defined expiry, the §6.3 orphan re-enumeration, the residual decision (§6.2), and the whole teardown test matrix. | **L** | Highest risk; own slice per direction. Not folded into S3. |
| **S6** | `Flush` drains the accumulator (§3.5) — both `FlushWithCallback` and the teardown flush. | S | Adjacent to S5 but a distinct contract (Java's flush) with its own test. |
| **S7** | Mock-timing test migration (§9), allocation-budget re-baseline, `ffi-marshalling.md` §A1/§A4/§A7 amendments (§5), STATUS entry, the §8.2 perf-deferral note. | M | Closes the accumulator work. |
| **S8a** | **Completion-pump cap** (§12.2): cap `DrainAll` at `SLOT_CAPACITY`, add the `RunLoop` inner drain loop (§12.2.1 — **the hang fix**), leave `DrainAndFaultRemaining` uncapped (§12.2.2), rewrite the `:59-62` paragraph (§12.4). Tests 24–26. | M | **Its own slice, on the completion side.** A regression here must be attributable to the cap, not entangled with the accumulator. The hang hazard makes this higher-risk than its size suggests. |
| **S8b** | **Reusable `IntPtr[SLOT_CAPACITY]` arrays** (§12.3): three preallocated fields, `count`-bounded everywhere, `ProcessBatch` static→instance, residual-3(b) wording narrowed. Test 27. | S | **Separate from S8a and gated on it being green.** S8a is a correctness fix; S8b is an allocation change that introduces a double-free hazard. Landing them together would make a fault ambiguous between the two. |

---

## 8 · Tests (performance is deferred — §8.2)

### 8.1 Required tests

Broker-free, on `AsyncMockProducer`, following the existing conventions.

**Timing / batching**
1. Sub-threshold release: N < threshold is released by the **timer** (assert the records arrive; assert the elapsed bound).
2. Exact-threshold release: N == threshold is released by the **signal**, not the timer (assert it completes well inside the window).
3. Chunking, both axes of §3.4's formula, asserting the `send_batch` call **count** (not merely that records arrive):
   - **(a) at the defaults** — a drained chain spanning **multiple nodes** produces exactly one `send_batch` per node, never one for the whole chain. A node at full `SLOT_CAPACITY` must be **one** call, which is what proves default parity with the anchor.
   - **(b) under a lowered chunk override only** — set `CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK` below `SLOT_CAPACITY` and assert `ceil(count / chunk)` calls for a node exceeding it. This axis is **unreachable at the defaults by construction** (§3.4), so the override is the only way to cover the splitting branch.
4. Env-var overrides take effect (window, threshold, bound), and are read once at construction.

**Backpressure**
5. Blocks at the bound, then releases when the drain resets the counter.
6. No lost wakeup: a drain completing *between* the check and the wait does not strand the sender.
7. Teardown cancels the gate — a `Send` blocked on backpressure completes (faulted) and `Close` returns.

**Lifetime (the memory-safety core)**
8. **Pin/unpin balance on every path**: append throws, accumulator closed, `SendBatch` throws, node abandoned by teardown, immediate-error path. Zero leaked `MemoryHandle`/`GCHandle`.
9. **Async surface only.** Mutation-after-send through the accumulator: on `AsyncMockProducer`, mutate the caller's buffer after `Send` returns but *before* the drain, and assert the produced record matches the value **at drain time**. ⚠ This is a deliberate **contract change** from Option C (D6, §4.7): under Option C the copy happened during `Send` so a post-`Send` mutation was invisible; under Option A it is visible until the drain. Do **not** write a sync counterpart — the sync path still copies during the call and has no such window (§4.7); test 22 covers that it stays that way.
10. Absent vs empty vs present key/value each produce the correct record **deferred** (the §4.2 static sentinel).
11. Topic interning: many records across few topics allocate O(topics) pins; a topic name whose bytes would be freed early is not corrupted (long/unicode topic, GC forced between `Send` and drain).
12. `GC.Collect()` forced between `Send` and the drain does not corrupt any record (the whole point of pinning).

**Immediate errors**
13. Per-record immediate error faults exactly that record's `Task`, fires exactly one delivery callback, frees the error handle, and the **surviving** records still reach the pump (compaction).

**Teardown / flush (S5/S6)**
14. **Close drains pending accumulator records** — they complete, not fault.
15. **Flush drains pending accumulator records** — assert the returned `Task`s are resolved *after* `Flush` returns (the test the anchor lacks: Python's `test_flush` asserts only "does not raise").
16. `Dispose` / `DisposeAsync` / `Close` / `CloseWithCallback` each drain — all four flavors.
17. Concurrent `Send` + `Dispose` churn: no hang, no crash, no leak.
18. Close while the batch thread is blocked in `send_batch`: returns within a bounded time.
19. `AsyncMockProducer.Clear()` with records in **both** the accumulator and the pump: teardown returns (the §6.3 regression).
20. Batch thread dies from an unhandled throw → pending records fault and teardown still returns (no unbounded wait).

**Contract preservation**
21. `IDeliveryCallback` exactly-once across every new path (extend `PublicProducerDeliveryCallbackTests.cs`).
22. The **sync** path is untouched: the existing `PublicSyncProducer*Tests` pass unmodified. Treat any required edit there as a design error.
23. Allocation budget (DoD §10) re-baselined: the accumulator node arrays are a new per-record cost. State the new marginal budget and why. ⚠ Read the memory note on marginal alloc tests before changing the harness.

**Completion-pump cap (S8a — §12.2). These are new test files against `SendCompletionPump`, which today has only the 3-test `Interop/SendCompletionPumpGateTests.cs`.**
24. A drain of **more than** `SLOT_CAPACITY` produces **multiple** `ProcessBatch` invocations, each of at most 1100. Assert the invocation count and each batch's size.
25. **⚠ THE HANG REGRESSION TEST — the one that catches a naive cap (§12.2.1).** Enqueue well over `SLOT_CAPACITY` (e.g. 3000) and then **stop enqueuing entirely**; assert **every** completion resolves. Without the inner drain loop this hangs deterministically on the 1901st record, so the test must have a **hard timeout and fail rather than hang** the suite. This is the single most important test in S8a.
26. Teardown with more than `SLOT_CAPACITY` queued still faults **every** TCS — i.e. `DrainAndFaultRemaining` was **not** capped (§12.2.2). Assert the count of faulted tasks equals the count enqueued.

**Reusable arrays (S8b — §12.3)**
27. **Stale-slot / double-free guard:** a large drain (near `SLOT_CAPACITY`) immediately followed by a **small** one (e.g. 2 records) completes correctly and frees exactly the small batch's handles. This is the test that catches a `Length`-instead-of-`count` bound; without it the reuse is unverified.

### 8.2 Performance: a tracked follow-up, NOT a gate on this phase (D3)

**Decision (D3, user, 2026-09-07): performance measurement is post-implementation. There is no
perf gate in this phase.** `make verify` / DoD §9 still apply in full — that is a
correctness-and-lint gate, and it is unaffected by this decision. Do not block any slice on a
perf number, and do not spend Actor time capturing one.

**The baseline is not lost, which is what makes deferral safe.** The comparison is a diff against
the parent commit, and the parent commit remains measurable at any later date. Nothing about this
phase destroys the ability to measure it.

**What must be captured when the follow-up runs** (recorded here so the deferral does not lose
the design of the measurement):

- **A low-rate / paced baseline on the Option C parent commit, captured first.** This is the
  measurement that matters and **no baseline for it exists**. At max rate the 0–10 ms stage-1
  window is noise against Option C's p50 ≈ 96 ms deep-pipeline latency (§3.7); at low rate
  stage-1 dominates and Option C has no stage-1 delay at all. Measuring only at max rate would
  produce a misleadingly clean result.
- **The known max-rate reference**, for the throughput side: Option C uncapped, async, 1 KB
  values — **642,504 msg/s · p50 96 ms · 237 MB RSS · 313% CPU · CPU-eff 2052** (the HTML doc's
  matched run). A throughput drop here would point at the §3.4 mutex hold, and the §3.2 row-5
  chunk constant is the one-line knob for it.
- **Expected shape of the result**, so a surprise is recognizable: async latency ≈ Option C's
  **plus** the 0–10 ms window; throughput roughly at parity; no improvement in p50 from the
  accumulator bound (§3.7 — it bounds upstream of the core, not the deep downstream pipeline).

**Apparatus** (already on this branch, `732e259f`): `bindings/dotnet/Makefile` targets
`producer-perf-test-dotnet` (`CLIENT_VERSION=3` → PerfV3, `=2` → PerfV2/ckd),
`test-integration-perf-dotnet` (Docker), `perf-unit-test-dotnet`; `PERF_TFM ?= net10.0`.
**Caveats:** the **full** perf leg is intermittently flaky (a shared broker plus in-container load
producers that are never stopped, which cascades), so **`net10.0` is the reliable leg**. Ask the
user for the recorded local-run recipe (`CLIENT_VERSION`, `True`/`False` casing, `LIMIT_RPS`
override) before running it.

### 8.3 DoD

All of `.claude/rules/definition-of-done.md` applies. Specifically in play:
**§10** (hot-path allocation audit — the send path is the hot path; see test 23);
**§12/CLAUDE.md §12** (zero-copy — this is what motivates pinning over copying);
**§3** (error-message content asserted, not just `is_err()`);
**§7** (new structs not in Java — the accumulator/pin types are binding-internal machinery with
the rationale in this plan, exactly as the pump was);
**§11** is consumer-only, N/A. `make verify` (or the dotnet leg) must be green.

---

## 9 · Corrections and additions to the incoming scoping

The 12 incoming work items are validated with these changes:

- **Item 5 (pinning, "L — the real cost")** — confirmed L, and split S2/S3 to isolate it. But the
  cost is **lower than estimated**: topic interning (§4.1) removes the third pin entirely, and
  `MemoryHandle` (§3.9) is a cleaner primitive than `GCHandle.Alloc`. The genuinely hard part is
  the exactly-once unpin across ~6 failure paths, not the pinning itself.
- **Item 9 (teardown, "L — highest risk")** — confirmed, and it is worse than stated: it must
  also re-run the pump-orphan enumeration (§6.3) and the waits must be **bounded**.
- **NEW — mock manual-completion timing shifts.** Recorded in agent memory as a known caveat of
  the C→A switch: *"`Send` then `CompleteNext` — the deferred send may not have reached the core
  yet, so some mock tests may need adjustment."* This affects the 8 tests in
  `PublicProducerMockControlTests.cs` and several of the 9 in-flight teardown tests in
  `PublicProducerSendTests.cs`. **Size M, and it is a known cost, not a risk.** The fix shape is
  an internal test-only "drain now and wait" hook, not a `Thread.Sleep`.
- **NEW — allocation-budget re-baseline** (test 23). The accumulator node arrays are a new
  per-record cost against an existing budget test. Size S, but it will fail the existing gate if
  ignored.
- **NEW — S0** (supersede the records first) and the **§5 rule amendments** as explicit
  deliverables. Item 12 covered the decision record and §A4/§A7; **§A1 was missing** and is the
  one actual rule conflict (§5.2).
- **NEW — §3.4, the coarse-mutex critical section.** Not in the incoming scoping and it is the
  most consequential technical finding. It did **not** end up setting the chunk size — D1 chose
  Python parity (`SLOT_CAPACITY` = 1100) and accepted the cost — but it is why the chunk exists as its own tunable
  constant, and it is the recorded rationale a future perf result would act on.
- **Item 12's "`ffi-marshalling.md` §A4/§A7"** — correct, plus §A1.

**Estimate.** The incoming estimate was 2–3 weeks. **Validated as ~2.5–3 weeks of Actor time for
S0–S8b** — S8a/S8b add ~2–3 days: small in code, but the hang hazard (§12.2.1) and the
double-free hazard (§12.3) each need a dedicated regression test against a file that currently has
almost no direct coverage. End-to-end **3–4 weeks** including the actor-critic loop, because:
- M11/P2 needed a whole follow-up phase plus four fixups after a clean review, and this phase is
  larger and touches memory safety and teardown — the two areas the recorded Critic findings
  cluster in.
- The N=51 pump-orphan blocker was found only by an empirical repro, and §6.3 requires repeating
  that exercise.
- **D3 removed the schedule wildcard.** The earlier 3.5–4.5 week figure carried the perf gate,
  which needed a broker and a fresh low-rate baseline against a flaky leg. With performance
  deferred (§8.2) that uncertainty leaves this phase.

---

## 10 · Plan location and STATUS

**This file:** `bindings/dotnet/design/history/M11/P3.1-producer-python-alignment/PLAN.md`.

A phase directory holds **only** `PLAN.md` and `COMMENTS.DONE.<N>.md` — nothing else (user
directive, 2026-09-07; the shape of every sibling, e.g. `P3-producer-send/`,
`P2.1-collapse-producer-teardown/`, `P4.1-producer-naming-cleanup/`). This phase is filed there
from the start rather than staged in `design/current/` and moved at close, so the **deviation
from the `PLAN-M14-producer-delivery-callback-parity.md` in-flight precedent is deliberate**.

One consequence to hold in mind, since the earlier draft of this section leaned on it: sitting in
`history/M11/` next to `P3-producer-send/` does **not** make this a shipped decision. It is
in flight until close. §2 is what establishes that it supersedes `P3-producer-send/`, not this
file's location. `COMMENTS.DONE.65.md` joins it as the review loop produces comments.

**`STATUS.md` needs two edits, not one:**
1. **Now (S0):** amend line 99's "No `ProducerRecord_t` mirror struct (that was Option A /
   `send_batch`), no managed accumulator bound." — it will be false the moment S1 lands.
2. **At close (S7):** a new dated `Milestone 11 / Phase 3.1` entry in the established format
   (DONE date, N, Mode A confirmation with the `git diff --stat` evidence over `src/**` /
   `src/ffi/**` / `confluent_kafka.h` / `cbindgen.toml`, plan pointer, branch, commits,
   deliverables) — plus updating the **accepted-residuals** section with §6.2's outcome.

---

## 11 · Resolved decision record — all seven settled

**All seven decisions were taken by the user on 2026-09-07. There are no open questions
gating the Actor.** Recorded here as settled inputs; the recommendations they overrode are kept
so the reasoning is auditable rather than erased.

| # | Decision (user, 2026-09-07) | One-line rationale | Where implemented |
|---|---|---|---|
| **D1** | **Chunk at Python's effective per-call maximum, `SLOT_CAPACITY` = 1100**, as its own tunable constant. *(Overrode the plan's recommended 256.)* **Amended 2026-09-07** — this row first read "1000"; see the note below. | **Exact Python parity.** The mutex-hold cost (now up to 1100/acquisition) is accepted and recorded rather than designed around. The constant exists so D3's deferred perf result is actionable as a one-line change; at the default it changes nothing. | §3.2 row 5, §3.4 |
| **D2** | **Window default: 10 ms.** *(Overrode the plan's recommended 1 ms.)* Tunable per §3.2; free-running timer kept. | **Python-faithful.** The 0–10 ms delay is an accepted cost, mitigated by tunability, and will be measured under D3. | §3.2, §3.3 |
| **D3** | **Performance is post-implementation. No perf gate in this phase.** | The baseline is not lost — the parent commit stays measurable — so deferring costs nothing and removes the phase's only schedule wildcard. DoD §9 / `make verify` still apply. | §8.2 |
| **D4** | **Accept** the mock manual-completion timing migration. | Inherent to deferring the send; the fix is a test-only drain-and-wait hook, never a `Thread.Sleep`. | §9, S7 |
| **D5** | **Bound the topic cache**, as recommended, with a stated eviction behavior. | Interning is O(distinct topics) permanent pins; an unbounded distinct-topic workload would otherwise grow it without limit. | §4.1 |
| **D6** | **Accept** the post-`Send` buffer-mutation visibility change, and **document it**. ⚠ **Async surface only** — see §4.7. | Inherent to deferring a zero-copy send; the alternative is the per-record copy CLAUDE.md §12 forbids. | §4.7, §8.1 test 9 |
| **D7** | **Keep Python's constant names.** | With D1 and D2 keeping Python's *values* too, the earlier "do the tuned constants still deserve the name?" question is moot: names, values and mechanism all match the anchor. The only net-new constant is the decoupled chunk (§3.2 row 5). | §3.2 |

**Amendment to D1 (2026-09-07, same day).** The row first recorded the chunk default as **1000**.
That was wrong, and the error originated in how the decision was framed rather than in the
decision: the options offered were "256 vs 1000", but **1000 is `SLOT_THRESHOLD`** — the early-wake
trigger and the backpressure bound — **not a per-call bound.** Python has no chunk constant at all;
its de-facto per-call maximum is one `BatchNode`, and a node fills to `SLOT_CAPACITY` = **1100**
(verified: `:806` allocates a new node only at `count == SLOT_CAPACITY`; `:585` sizes the flat array
at `SLOT_CAPACITY`; `:593` issues one call per node). The user's stated intent was **exact parity
with Python's constant values**, so the faithful realization of that intent is **1100**. Corrected
throughout. The consequence is strictly simplifying: the `ceil(count / chunk)` split becomes
unreachable at the defaults, so the "1000 + 100 two-call" divergence an earlier draft recorded
**no longer exists**.

**Note on D1 + D2 + D7 together.** The plan originally proposed tuning two constants downward on
.NET-specific grounds. The user chose parity on **both**, so §3.2 now keeps Python's names *and*
values, and the sole deliberate addition is the chunk constant — whose *default is Python's
effective value*, making it a name without a behavior change. That leaves the phase's deviation
list (§3) **shorter** than the draft's: the remaining deviations are all structural (sync path
inline, flush drain, teardown handshake, tunability) rather than magnitudes.

**Standing principle for this phase (user, 2026-09-07): no divergence from Python's constant
values.** Any constant the implementation needs must either take Python's value or be recorded in
§3 as an explicit deviation with a rationale. §12 records the audit against this principle.

**Also flagged, not for this phase:** the complete fix for the close/backpressure residual is
finer-grained locking in `src/ffi/producer.rs` (**Mode B**) — escape hatch #2 in the recorded
memory. §3.6 states how much of that residual this phase does and does not fix.

---

## 12 · Constant-values audit (against the D1/D7 standing principle)

Every numeric constant this phase introduces or relies on, checked against the anchor. Verified
against `bindings/python/_confluentkafka.c` while writing this section — not recalled.

| Constant | Python value / site | This plan | Verdict |
|---|---|---|---|
| drain threshold | `PRODUCER_RECORD_SLOT_THRESHOLD 1000` `:19` | 1000 | ✅ identical |
| node capacity | `PRODUCER_RECORD_SLOT_CAPACITY (THRESHOLD + 100)` = 1100 `:20` | 1100, expressed as `threshold + 100` | ✅ identical, and derived the same way |
| backpressure bound | `PRODUCER_MAX_ACCUMULATED_RECORDS = SLOT_THRESHOLD` = 1000 `:27` | 1000, expressed as `= threshold` | ✅ identical, and coupled the same way |
| linger window | bare literal `10000000` ns = 10 ms `:535` | 10 ms | ✅ identical value (now named + tunable, §3.2) |
| per-`send_batch` chunk | *no constant*; effective max = one node = `SLOT_CAPACITY` `:585`/`:806`/`:593` | 1100 | ✅ identical **effective** value; the *name* is net-new (§3.2) |
| completion-batch bound | one `BatchNode` per `get_all`; arrays sized `SLOT_CAPACITY` `:429-430`, one node per loop iteration `:480-513` | 1100 (`DrainAll` capped, §12.2) | ✅ identical — **brought into scope 2026-09-07**; was the one pre-existing divergence |
| **topic-cache cap** | **no counterpart** — Python has no topic cache at all (a per-record `topic_owned` malloc, `:30-36`) | **1024**, insert-only, per-record fallback beyond (§4.1) | ⚠ **the sole remaining deviation** — see §12.1 |

**Result: the constant set is fully Python-aligned.** Every constant with a Python counterpart now
takes Python's value. The only non-Python number in the phase is the topic-cache cap, which has no
counterpart to align to (§12.1).

### 12.1 The sole remaining deviation: the topic-cache cap (1024)

This is **not** a divergence *from* a Python value — there is no Python value to diverge from.
Python allocates an owned `topic_owned` copy **per record object** and frees it with the record,
so it needs no cache and no cap. .NET cannot copy the topic per record without adding exactly the
per-record allocation §A4/CLAUDE.md §12 exist to prevent, so §4.1 interns one permanently-pinned
buffer **per distinct topic** instead — a net-new mechanism, which necessarily brings a net-new
constant.

**Recorded per the D1/D7 principle**, with the rationale: the cap bounds the only unbounded thing
the mechanism introduces (permanent pins on an unbounded distinct-topic workload), and the
beyond-cap fallback is a per-record pinned topic buffer — i.e. it degrades to *Python's* shape
(a per-record topic allocation) rather than failing. That makes 1024 a **safety valve on a
.NET-only mechanism**, not a tuning knob competing with an anchor value. If a reviewer prefers,
the equally faithful alternative is no cache at all (per-record topic pin, 3 pins per record);
the cache is a strict improvement on that, and the cap is what keeps it one.

### 12.2 The completion-side bound — IN SCOPE (user, 2026-09-07)

**Decision: cap the completion pump's drain at `SLOT_CAPACITY` (1100).** Rationale: full alignment
with Python's constant values, which is the standing principle (§11). This was raised as a
follow-up note; the user brought it in scope.

**The parity citation.** Python bounds its completion batch: the poll-futures thread processes
**one `BatchNode` at a time** (`:480-513` — `Producer_complete_callbacks(..., current_pending_batch->count)`
per iteration) and sizes its `get_all` output arrays at `SLOT_CAPACITY`
(`metadata_ptrs[PRODUCER_RECORD_SLOT_CAPACITY]` / `error_ptrs[PRODUCER_RECORD_SLOT_CAPACITY]`,
`:429-430`), so its per-call completion batch is ≤ 1100. .NET's `SendCompletionPump.DrainAll()`
(`:275-284`) drains the **entire** `ConcurrentQueue` and `ProcessBatch` allocates three
`IntPtr[count]` over whatever it drained — unbounded by construction.

Scope is **the cap and its direct consequences only** (§1.5's carve-out table). This is not a pump
redesign.

#### ⚠ 12.2.1 THE HANG HAZARD — a naive cap is a guaranteed hang, not a race

**Read this before writing the cap.** Verified against `SendCompletionPump.cs`:

```csharp
private void RunLoop() {
    while (true) {
        _signal.Wait();                       // :233  BLOCKS — no timeout, no token
        _signal.Reset();                      // :237  reset BEFORE the drain (deliberate)
        if (_stopping) break;                 // :239
        List<PendingSend> batch = DrainAll(); // :246  TODAY: empties the queue completely
        if (batch.Count > 0) { ProcessBatch(batch); /* ... */ }
    }
}
```

`DrainAll` is `while (_queue.TryDequeue(out ...)) batch.Add(...)` — it always leaves the queue
empty. **The loop's correctness depends on that.** Cap it without changing the loop and leftovers
strand permanently:

> queue holds 3000 → capped drain takes 1100 → **1900 remain** → next iteration hits
> `_signal.Wait()`, which was already `Reset()` at `:237` and is only `Set()` by a new `Enqueue`.
> With no further sends, those **1900 completions and every awaiting `Task` hang forever.**

Not a race — deterministic, and it needs no concurrency to reproduce.

**The fix — an inner drain loop, which is also the anchor's shape.** Keep draining and processing
capped batches until a drain comes back **short** (fewer than the cap), and only then fall through
to `_signal.Wait()`. That is precisely what Python does: its poll thread completes one node, then
advances `next_pending_batch = current_pending_batch->next_batch` and `cnd_wait`s **only** while
`next_pending_batch == NULL` (`:504-506`).

**Do NOT "fix" it by calling `_signal.Set()` when items remain.** The inner loop is clearer, is
what the anchor does, and does not depend on reasoning about a self-signal racing a `Reset`.

**The reset-before-drain rationale is unchanged and must be preserved.** The existing comment at
`:235-236` explains it: an `Enqueue` during or after the drain re-`Set`s the event, so its `Set`
lands *after* this `Reset` and the next `Wait` returns — no lost wakeup. That still holds under the
inner loop (the inner loop handles *known* leftovers; the `Reset` ordering handles *concurrently
arriving* ones). **Say so in the code**, so the next reader does not re-derive it or "simplify" the
ordering away.

#### 12.2.2 Cap `DrainAll` ONLY — never `DrainAndFaultRemaining`

An easy and damaging over-application. `Stop`'s terminal drain
(`DrainAndFaultRemaining`, `:468-486`) **must drain everything**:

- It **faults** rather than calling `get_all`, so it allocates **none** of the three marshalling
  arrays — the cap's entire motivation is absent.
- It is the last thing that ever touches the queue. Capping it **leaks uncompleted `TaskCompletionSource`s**
  and their future handles: their awaiters hang forever and the handles are never destroyed.

Make the asymmetry explicit in the code and in the review brief: **`DrainAll` is capped;
`DrainAndFaultRemaining` is not.**

### 12.3 Consequence taken: three reusable `IntPtr[SLOT_CAPACITY]` arrays

With the drain bounded, `ProcessBatch`'s three `new IntPtr[count]` allocations become bounded, so
they can be **three arrays allocated once as fields on the pump** instead of per drain. `RunLoop`
is single-threaded, so no synchronization is needed.

**Taken, because it is strictly more faithful than the status quo**: the anchor uses fixed-size
**stack** arrays (`:429-430`) and allocates nothing per batch. It also removes a per-drain
allocation against the DoD §10 gate.

**Three hazards, all of which must be handled explicitly:**

1. **⚠ Stale pointers beyond `count` are a double-free.** A reused array retains the previous
   drain's handle values past the current `count`. Any code reading `Length` instead of `count`
   would treat those as live handles and free them a second time. **Every loop and every
   `destroy_all` must be bounded by `count`, never `Length`** — and the arrays should additionally
   be cleared for the `0..count` range on entry, so a partially-filled batch cannot read a stale
   slot. A test must cover a large drain followed by a small one.
2. **`ProcessBatch` is `static` today** (`:312`). It becomes an instance method. That is a shape
   change to the most safety-critical method in the pump, which is why §12.4 gives it its own
   slice rather than folding it into the cap.
3. **It narrows a recorded residual, so the residual text must be updated.** Residual 3(b)
   (`IDeliveryCallback.cs:162-181`) explicitly names *"the pump threw while setting the batch up
   … the marshalling arrays, allocated outside the processing `try`"* as one of its two triggers.
   Preallocating removes that trigger; the other (*"the `get_all` P/Invoke itself against a stale
   native"*) remains, so the residual **narrows but does not vanish**. `ProcessBatch`'s
   allocation-`catch` (`:318-343`) becomes unreachable for the arrays and must be reconciled, not
   left as misleading dead code. Amend the residual wording; do **not** delete the residual.

**Deliberately NOT taken (recorded so it is a decision, not an omission):** `DrainAll`'s
`List<PendingSend>` is still allocated per drain. It could be reused too (`Clear()` + re-`Add`,
with `Count` authoritative so there is no stale-slot hazard), but it is a fourth object in a slice
already touching the pump's safety-critical core, and it is not what the parity argument is about.
Follow-up.

### 12.4 The stale `:59-62` backpressure paragraph must be rewritten regardless

Independent of the cap. `SendCompletionPump.cs:59-62` currently reads:

> "The queue is structurally unbounded but practically bounded by the core's `buffer.memory`
> backpressure: **the inline `Producer_send` on the caller thread blocks up to `max.block.ms`**
> when the core buffer is full, so callers cannot outrun the drain (PLAN §4 decision 4). No managed
> bound / hand-cap is added."

Under Option A the **mechanism is false** — the caller no longer calls `Producer_send`; the **batch
thread** does, and it is the batch thread that blocks on `buffer.memory` while the caller is
bounded by the accumulator instead (§4.6). "No managed bound / hand-cap is added" also becomes
false twice over: the accumulator bound, and this cap.

**The conclusion still holds and must be kept:** the queue remains practically bounded by
`buffer.memory`, because a future only enters it *after* the core has accepted its record — so
whatever bounds acceptance bounds the queue. **Rewrite the mechanism, keep the conclusion, add the
cap.** Do not let the Actor delete the paragraph: it is the answer to "why is an unbounded queue
acceptable here?", and that question is still live.
