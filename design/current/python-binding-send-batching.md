# Python binding: send-path batching and its latency profile

> Status: analysis only — no code change proposed here. Documents how the Python
> binding's C extension batches records in front of the Rust accumulator, the
> resulting latency profile, two open observations, and what it implies for the
> .NET binding.
>
> Corpus: `bindings/python/_confluentkafka.c`, `bindings/python/producer.py`,
> `src/ffi/producer.rs`, `src/producer/internals/record_accumulator.rs` as of
> branch `prashah_dev_dotnet_binding_producer`.

## Summary

The Python binding does **not** hand each `send()` straight to the core. Its
hand-written C extension inserts a **second accumulator** in front of the Rust
one, with its own batching thresholds and its own timer. A record therefore
passes through **two independent buffering stages in series**:

```
Python send()  ──►  [STAGE 1: binding accumulator]  ──►  send_batch()
                    1000 records / 10 ms                       │
                    hardcoded, untunable                       ▼
                                            [STAGE 2: Rust accumulator]  ──► wire
                                             batch.size / linger.ms
                                             configurable
```

Stage 1 exists to amortise the Python↔C boundary cost (GIL acquire/release plus
the call itself) across ~1000 records instead of paying it per record. That is a
real and significant win in Python.

The cost is that **stage 1 adds 0–10 ms of delay that no configuration can
influence**, and at low-to-moderate throughput this dominates `linger.ms`.

Stage 1 is **Python-specific**. The .NET binding has no equivalent: it calls the
singular `Producer_send` inline on the caller's thread, so `linger.ms` is its
only delay.

## Stage 1: the binding accumulator

### Thresholds (`_confluentkafka.c:19-27`)

```c
#define PRODUCER_RECORD_SLOT_THRESHOLD 1000
#define PRODUCER_RECORD_SLOT_CAPACITY (PRODUCER_RECORD_SLOT_THRESHOLD + 100)

// Backpressure bound: once this many records are accumulated but not yet taken
// by the send task, the producer is "full" and further enqueuing should wait
// until the send task drains a batch.
#define PRODUCER_MAX_ACCUMULATED_RECORDS PRODUCER_RECORD_SLOT_THRESHOLD
```

| constant | value | role |
|---|---|---|
| `SLOT_THRESHOLD` | 1000 | early-wake trigger, and the backpressure bound |
| `SLOT_CAPACITY` | 1100 | per-`BatchNode` array size (100 slots of slack) |
| the 10 ms window | `:535`, a bare literal | timeout trigger — **no `#define`, no config key** |

The 100-slot gap between threshold and capacity is deliberate: between the
`cnd_signal` at 1000 and the send thread actually acquiring the mutex, more
records can land in the same node without forcing a fresh allocation.

### Data structures (`_confluentkafka.c:360-398`)

A linked list of fixed-size arrays. `send()` appends to the **tail**; the send
thread takes the whole chain from the **head**.

```
       next_batches_to_send                    last_accumulating_batch
              │                                          │
              ▼                                          ▼
        ┌──────────┐        ┌──────────┐        ┌──────────┐
        │ BatchNode│───────►│ BatchNode│───────►│ BatchNode│
        │ 1100 recs│        │ 1100 recs│        │  247 recs│ ◄── new records land here
        └──────────┘        └──────────┘        └──────────┘
```

Each `BatchNode` carries five parallel arrays (`:361-369`): the Python record
objects, the `ProducerRecord_t` pointers, the completion callbacks, and — filled
in later by `send_batch` — the futures and immediate errors.

### Threads

| thread | function | role |
|---|---|---|
| caller | `py_Producer_send` `:777` | append to the accumulator, return |
| send | `Producer_send_thread` `:523` | batch → `Producer_send_batch` |
| poll-futures | `Producer_poll_futures_thread` `:480` | `get_all` → resolve futures |

The poll-futures thread is spawned *by* the send thread (`:526-527`) and joined
by it on shutdown. Both are created at producer construction (`:694` mock,
`:772` Kafka).

## The wake-up rule

The send thread waits in `cnd_timedwait` while **all three** hold
(`_confluentkafka.c:533-551`):

```c
mtx_lock(&producer->record_batches_mutex);        // :533
now = current_time_ns();                          // :534
timeout = now + 10000000; // 10ms                 // :535  <-- deadline set HERE,
while ((                                          // :536      before any record
    producer->next_batches_to_send == NULL        //            is known to exist
    || producer->next_batches_to_send->count < PRODUCER_RECORD_SLOT_THRESHOLD
) && timeout > now && !producer->closed) {        // :539
    ...
    cnd_timedwait(&producer->record_batches_new_record_cnd,
                  &producer->record_batches_mutex, &ts);   // :548
    now = current_time_ns();                      // :550
}
```

So it releases on **whichever comes first**: 1000 records, or the 10 ms timeout.

### ⚠ The timer is free-running, not started by the first record

`:535` computes the deadline at the **top of the send thread's loop iteration**,
from the send thread's own clock. It has no relationship to when your records
arrive. While idle the thread simply cycles through back-to-back 10 ms windows,
and your records land at an arbitrary phase within whichever window is already
open.

**Consequence:** for a sub-threshold batch the delay is uniformly distributed
over 0–10 ms (mean ~5 ms), not a predictable 10 ms. Identical code and identical
record counts can differ by two orders of magnitude run to run.

### The only three wake-up signals

Verified exhaustively — `record_batches_new_record_cnd` is signalled at exactly
three sites:

| site | condition | drains pending records? |
|---|---|---|
| `:826` in `py_Producer_send` | tail reaches **1000** | yes |
| `:967` in `py_Producer_shutdown` | `close()` | yes — sets `closed`, then drains |
| `:895` in `py_Producer_test_set_paused` | test-only | n/a |

Notably **absent**: `flush()`. See "Observation 1".

## Worked example: 500 records at t=0.1 ms

The decisive line is `:825` — the signal is gated on `>= 1000`, so 500 records
never wake the send thread:

```c
producer->last_accumulating_batch->count++;                             // :822
producer->accumulated_records++;                                        // :823

if (producer->last_accumulating_batch->count >= PRODUCER_RECORD_SLOT_THRESHOLD) {
    cnd_signal(&producer->record_batches_new_record_cnd);               // :826  <-- NOT REACHED
}
full = producer->accumulated_records >= PRODUCER_MAX_ACCUMULATED_RECORDS; // :830 -> false
```

### Case A — records land early in the window (worst case)

```
       MAIN THREAD                          SEND THREAD
       ───────────                          ───────────

t=0.0ms                                     :533  lock mutex
                                            :535  timeout = 0.0 + 10ms = 10.0ms  <-- window opens
                                            :536  cond? batches==NULL -> TRUE
                                            :548  cnd_timedwait(deadline=10.0ms)
                                                  └─ SLEEPS, releases mutex
                                                       │
t=0.1ms  send(r1)  :805 lock  ◄─────────────────────────┘ (mutex free)
                   :806 malloc node1
                   :819 append, count=1
                   :825 1 >= 1000? NO -> no signal
                   :831 unlock
           ⋮
         send(r500) :822 count=500
                    :823 accumulated_records=500
                    :825 500 >= 1000? NO   -> no signal     SEND THREAD STILL ASLEEP
                    :830 full = false      -> no backpressure
                    returns False

         [Python now holds 500 unresolved Futures]
         … nothing happens for 9.9 ms …
                                                       │
t=10.0ms                                    :548  cnd_timedwait TIMES OUT ◄─────────┘
                                            :550  now = 10.0ms
                                            :536  cond? count(500) < 1000 -> TRUE
                                            :539  timeout(10.0) > now(10.0) -> FALSE
                                                  └─► EXIT LOOP  (released by the TIMER)
                                            :562  batches != NULL -> proceed
                                            :567  take node1 (500 records)
                                            :573  accumulated_records = 0
                                            :587  flatten 500 structs
                                            :593  send_batch(…, 500, …)
                                                  └─► records finally reach Rust
```

**Waited 9.9 ms**, released by timeout, not by signal.

### Case B — records land late in the window (best case)

```
t=0.0ms                                     :535  timeout = 10.0ms  <-- window already open
                                            :548  sleeping…
t=9.8ms  send(r1..r500)                           (same path, no signal)
t=10.0ms                                    :548  TIMES OUT -> drain -> send_batch
         └─ waited 0.2 ms
```

Same code, same 500 records: **9.9 ms vs 0.2 ms**, decided purely by phase.

### Contrast — 1000 records

```
t=0.1ms  send(r1..r999)  :825 no signal
         send(r1000)     :822 count=1000
                         :825 1000 >= 1000? YES -> cnd_signal!  -> WAKES send thread
                         :830 full = TRUE -> Python blocks on space.result() / await space

t=0.1ms                                     :548  cnd_timedwait RETURNS (signalled)
                                            :536  count(1000) < 1000 -> FALSE -> EXIT
                                            :567  take, :573 reset, :579 fire space cbs
                                                  └─► unblocks the Python sender
                                            :593  send_batch(…, 1000, …)
```

Sub-millisecond.

### The resulting latency profile is bimodal

| records in the window | stage-1 delay |
|---|---|
| ≥ 1000 | ~0 (signalled immediately) |
| 1 … 999 | **0–10 ms, uniform** (timer only) |

A workload that produces a few hundred records per 10 ms window sits in the
worst part of this curve.

## The drain (`_confluentkafka.c:562-621`)

```c
head_batch_node = producer->next_batches_to_send;   // :567  take the whole chain
producer->next_batches_to_send  = NULL;             // :569
producer->accumulated_records   = 0;                // :573  backpressure released
Producer_take_space_cbs_locked(...);                // :576
mtx_unlock(&producer->record_batches_mutex);        // :577

Producer_fire_and_free_space_cbs(...);              // :579  unblock waiting senders

for (int i = 0; i < batch_node->count; i++)         // :586
    flat_records[i] = *batch_node->producer_structs[i];   // :587

kafka_producer_Producer_send_batch(                 // :593  ONE FFI call per node
    producer->producer, flat_records, batch_node->count,
    batch_node->futures, batch_node->batch_errors);
```

One `send_batch` call **per `BatchNode`**, walking the chain — not one call for
the whole chain. A 2447-record chain is 3 calls (1100 + 1100 + 247).

Records whose `batch_errors[i]` came back non-null are completed immediately on
the send thread and compacted out of the arrays (`:600-621`); the rest are handed
to the poll-futures thread.

## Backpressure

`accumulated_records` counts everything appended but not yet taken; the bound is
also 1000. `send()` returns that as a boolean `full` (`:830`), and Python then
waits (`producer.py:378-386` sync, `:690-700` async):

```
send() returns full=True   when accumulated_records >= 1000
   │
   └─► Python waits: space.result()  (sync)  /  await space  (async)
                                │
        send thread's drain ────┘  :573 resets counter, :579 fires the space callbacks
```

Because both the early-wake signal and the backpressure bound trigger at the
same record (1000), hitting the bound coincides with waking the send thread. The
check-and-register in `py_Producer_on_space_available` (`:843`) runs under the
*same* `record_batches_mutex` the drain holds, so there is no lost-wakeup window;
and if the drain already ran it returns `True` immediately and the caller never
waits (`:855-859`).

## Stage 2: what `linger.ms` does and does not add

The two stages are strictly **in series** — the stage-2 clock cannot start until
`send_batch` has created the batch, i.e. at t=9.9 ms in case A above.

But `linger.ms` is a **ceiling, not a fixed cost**
(`src/producer/internals/record_accumulator.rs:1091-1098`, Java-faithful):

```rust
let expired = waited_time_ms >= time_to_wait_ms;   // linger elapsed
let sendable = full                                // batch.size hit: SHORT-CIRCUITS linger
    || expired
    || exhausted
    || self.closed.load(Ordering::Relaxed)
    || self.flush_in_progress()
    || transaction_completing;
```

Defaults (`src/producer/producer_config.rs:236-237`): `batch_size = 16384`,
`linger_ms = 5`.

A burst of 500 records delivered in one `send_batch` will usually trip `full`:
16384 / 500 ≈ 33 bytes per record, so any record larger than ~33 bytes fills the
per-partition batch on arrival and **linger contributes nothing**. The linger is
only paid on a non-full tail batch — tiny records, or records spread thin across
many partitions.

### Total, for the 500-record case-A example

```
├──────── 9.9 ms ────────┤├── 0 to linger.ms ──┤
   stage 1 (untunable)      stage 2 (usually 0)
```

With `linger.ms=3`: **9.9 ms guaranteed, 12.9 ms worst case, ~9.9 ms typical.**

### The headline

Setting `linger.ms` low to chase latency buys less than it appears to:

```
├──────── ~5 ms avg, up to 10 ms ────────┤├── 0–3 ms ──┤
        UNTUNABLE (stage 1)                 your knob
```

The configurable knob controls the smaller — and more often skipped — half of
the delay.

## Observation 1: `flush()` does not drain the stage-1 accumulator

**Not yet reproduced against a broker. Recorded as an open question, with the
static evidence below.**

`flush()` goes straight to the Rust producer and never touches the C accumulator:

```python
def flush(self):                                    # producer.py:406
    self._run_sync(lambda cb: _lib.Producer_flush_async(self.c_producer, cb), ...)
```
```c
static PyObject* py_Producer_flush_async(...) {      // _confluentkafka.c:1238
    kafka_producer_Producer_flush_async(producer->producer, ...);   // Rust only
}                                                     // no cnd_signal, no drain
```

The same is true of the blocking `py_Producer_flush` (`:1119`).

Evidence:

1. `record_batches_new_record_cnd` has exactly three signal sites (`:826`,
   `:895`, `:967`) — none in either flush entry point.
2. `py_Producer_shutdown` (`:945`) *does* signal (`:967`) and *does* drain
   pending records before joining, so the close path is correct. The mechanism
   exists; flush just does not use it.
3. `flush_in_progress()` is itself a linger short-circuit in the `sendable` list
   above — so a Rust-side flush correctly forces out everything **it can see**.
   It cannot see records still sitting in stage 1.

Expected consequence:

```
t=0.1ms  send() x 500   -> 500 records in the stage-1 accumulator
t=0.2ms  flush()        -> Rust flush: nothing pending there yet -> returns fast
         ^^^^^^^^^^^^^^^^^^ reports "done" while 500 records are still buffered
t<=10ms  send thread drains -> send_batch -> records reach Rust -> delivered later
```

Java's contract is that `flush()` blocks until every **previously sent** record
has completed. From the caller's view those 500 *were* sent — `send()` returned.

Why this is largely masked today: the window is bounded (~10 ms plus delivery),
and callers typically also await the returned futures, which does block
correctly. The existing coverage does not probe it — the only flush tests assert
nothing beyond "does not raise":

```python
def test_flush():                                   # test/unit/test_producer.py:275
    with MockProducer(auto_complete=True) as p:
        p.send(ProducerRecord("test-topic", b"v"))
        p.flush()  # Should not raise
```

Nothing asserts the future resolved. The async counterpart (`:592`) is the same
shape.

If confirmed, the fix shape mirrors `py_Producer_shutdown`: signal the send
thread and wait for the accumulator to drain **before** calling into the Rust
flush.

## Observation 2: the stage-1 delay is untunable and undocumented

The 10 ms lives at `_confluentkafka.c:535` as a bare literal — no `#define`, no
config key, not surfaced through `ProducerConfig`. A user setting `linger.ms`
cannot see or influence it, and nothing in the Python docstrings mentions a
second buffering stage.

Both thresholds are also fixed: 1000 records is not derived from `batch.size`
or `buffer.memory`, so the stage-1 shape is identical for a 10-byte and a
10 KB record workload.

This is a deliberate design trade (boundary-crossing cost is the dominant
Python overhead), not an oversight — but it is currently an *undocumented* one,
which makes low-throughput latency measurements hard to interpret.

## Implications for the .NET binding

The .NET producer is **Option C**, recorded at
`bindings/dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.cs:2237-2243`:

> Option C — inline pull-pump: the SINGULAR `Producer_send` is called INLINE on
> the caller thread … No `ProducerRecord_t` mirror struct (**that is send_batch /
> Option A**), no per-send callback (that is `send_async` / Option B).

So .NET already has Python's **third** thread — `SendCompletionPump` is the
analog of `Producer_poll_futures_thread`, with the same batched `get_all`. What
it lacks is the **middle** (send/accumulator) thread.

Porting Python's stage 1 to .NET ("Option A") would need:

| Python piece | .NET equivalent | status |
|---|---|---|
| `next_batches_to_send` + mutex | a `SendAccumulator` | new |
| `Producer_send_thread` | a send-batch `Thread` | new |
| `BatchNode.producer_structs[]` | `ProducerRecordNative[]` mirror struct | new |
| `full` + `on_space_available` | one `SemaphoreSlim` | new |
| `Producer_poll_futures_thread` | `SendCompletionPump` | **exists** |
| `get_all` / `destroy_all` | `GetAll` / `DestroyAll` | **exists** |
| `Py_INCREF(record)` | *no equivalent* — see below | **blocker** |

Two arguments against it, in order of weight:

1. **It would import the 0–10 ms untunable floor.** .NET's `linger.ms` is
   currently the *only* delay and is fully honoured. This is a user-visible
   latency regression, not merely an implementation cost.

2. **Python's lifetime trick does not port.** Python keeps the record alive with
   `Py_INCREF` and hands Rust a raw pointer into the live `PyBytes`; CPython
   never moves heap objects, so this is free. The .NET GC compacts. Today .NET
   sidesteps the problem entirely — `ProducerSendMarshal` uses call-scoped
   `fixed` pins (`:64-75`) and the core copies key/value synchronously inside
   `Producer_send` (`src/ffi/producer.rs:262-281`), so the pin ends when the
   call ends. Deferring the send by ~10 ms would require either one
   `GCHandle.Alloc(..., Pinned)` per in-flight record (up to 2000 pins,
   fragmenting the GC heap) or a per-record copy into a pooled buffer — which
   reintroduces exactly the copy CLAUDE.md §12 forbids.

The upside — one P/Invoke per ~1000 records instead of per record — is also
worth far less in .NET: a P/Invoke is ~5 ns with no GIL to contend for, whereas
in Python the boundary crossing is the actual bottleneck. Note the batching saves
only the *crossings*: inside one `send_batch`, `send_batch_inner`
(`src/ffi/producer.rs:1504-1568`) still loops record-by-record doing
`rt.block_on(producer.send(rec))`.

`kafka_producer_Producer_send_batch` is already exported
(`src/ffi/producer.rs:1609`) but not declared in `NativeMethods`, so Option A
would be **Mode A** — .NET-only, no Rust change.

## Code reference index

### `bindings/python/_confluentkafka.c`

| lines | what |
|---|---|
| `19-27` | thresholds and backpressure bound |
| `30-36` | `ProducerRecordObject` (owns key/value refs, holds `record_struct`) |
| `360-369` | `BatchNode` — the five parallel arrays |
| `372-398` | `Producer` — both queues, both mutexes, both condvars, both threads |
| `401-420` | `Producer_complete_callback` — raw handles to Python, DECREFs |
| `422-444` | `Producer_complete_callbacks` — phase 1 `get_all` (no GIL), phase 2 dispatch |
| `450-478` | `Producer_fire_and_free_space_cbs` / `..._take_space_cbs_locked` |
| `480-513` | `Producer_poll_futures_thread` |
| `523-655` | `Producer_send_thread` |
| **`533-551`** | **the wait loop — `:535` is the 10 ms literal** |
| `555-560` | test-only pause |
| `562-579` | take the chain, reset backpressure, fire space callbacks |
| `583-598` | flatten + `Producer_send_batch` |
| `600-621` | immediate-error completion + array compaction |
| `645-654` | shutdown tail: `send_completed`, Rust `flush`, join poll thread |
| `777-834` | `py_Producer_send` — **`:825-827` the signal, `:830` the `full` flag** |
| `843-880` | `py_Producer_on_space_available` |
| `945-985` | `py_Producer_shutdown` — **`:967` signals and drains on close** |
| `1119-1138` | `py_Producer_flush` (blocking) — no accumulator drain |
| `1238-1246` | `py_Producer_flush_async` — no accumulator drain |

### `bindings/python/producer.py`

| lines | what |
|---|---|
| `128-147` | `_invoke_on_delivery` — callback obligation, exceptions swallowed |
| `150-163` | `_completion_to_python` — takes ownership of both C handles |
| `334-386` | `Producer.send` — `:373` the `full` flag, `:378-386` blocking backpressure |
| `406-411` | `Producer.flush` |
| `609-622` | `AsyncProducer._resolve_future` |
| `631-637` | `AsyncProducer._drain` — coalesced completion resolution |
| `650-702` | `AsyncProducer.send` — `:670-682` coalescing `cb`, `:690-700` awaited backpressure |
| `736-741` | `AsyncProducer.flush` |

### Rust core

| location | what |
|---|---|
| `src/ffi/producer.rs:1483-1571` | `send_batch_inner` — per-record loop, zero-copy borrow |
| `src/ffi/producer.rs:1609` | `kafka_producer_Producer_send_batch` |
| `src/ffi/producer.rs:1954` | `FutureRecordMetadata_get_all` |
| `src/producer/internals/record_accumulator.rs:1091-1098` | the `sendable` decision |
| `src/producer/producer_config.rs:236-237` | `batch_size = 16384`, `linger_ms = 5` |

### .NET binding

| location | what |
|---|---|
| `Internal/Interop/NativeMethods.cs:2237-2243` | the Option A / B / C decision record |
| `Internal/NativeProducer.cs:373` | `SendViaPump` (async path) |
| `Internal/NativeProducer.cs:579` | `Send` (sync path, blocking `get`) |
| `Internal/SendCompletionPump.cs` | the one existing pump thread |
| `Internal/Interop/ProducerSendMarshal.cs:64-75` | call-scoped `fixed` pins |
