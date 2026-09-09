# Why the soak's memory spikes during a broker roll

*A short explainer. Full investigation with raw measurements: `COMMENTS.0.md` on
`dev/python-soak-client`.*

## What we saw

The high-throughput soak (`testid=python-soak-ht`, 80 msg/s × 10 KB records) ran
flat at ~170 MiB RSS for hours, then during a broker roll spiked to **~790 MiB**.
Four more spikes followed over the next eight hours.

The spike itself is not the worrying part. **The baseline never came back** — it
stepped up to ~240 MiB and ratcheted a little higher after each subsequent
event. Extrapolated over a two-week batch, that trend ends in an OOM kill.

## The cause: 43 KiB of bookkeeping per 10 KiB record

The Python bindings batch outgoing records into a linked list of `BatchNode`
structs (`bindings/python/_confluentkafka.c:224`). Each node is:

```c
typedef struct BatchNode {
    int count;
    ProducerRecordObject*             producer_records[1100];
    kafka_producer_ProducerRecord_t*  producer_structs[1100];
    PyObject*                         complete_cbs[1100];
    kafka_producer_FutureRecordMetadata_t* futures[1100];
    kafka_common_KafkaError_t*        batch_errors[1100];
    struct BatchNode* next_batch;
} BatchNode;
```

Five arrays of 1,100 pointers = **44,016 bytes, allocated in full whether the
node holds 1 record or 1,100.**

The send task drains every 10 ms. At 80 msg/s that's fewer than one record per
drain — so in practice **one node per record**. Each 10 KiB record drags 43 KiB
of mostly-empty bookkeeping behind it.

Measured at peak: 8,132 blocks of 44,032 bytes = **341.4 MiB of `BatchNode`**
against **92.4 MiB of actual payload**. The bookkeeping outweighs the data it
describes by 3.7:1.

## The same thing, visually

**Every record travels in a box 4,400× bigger than it needs.** The sender
empties the box every 10 ms; at 80 msg/s a record arrives every 12.5 ms, so the
box always ships before a second record arrives — one record per box, always.

```
  ┌──────────────────────────────────────────────────┐
  │  slot[0] slot[1] slot[2] ......... slot[1099]    │  ← ×5 arrays
  │    ▲                                              │
  │  1 record.        1,099 slots sit empty.          │
  └──────────────────────────────────────────────────┘
              44,016 bytes, regardless
```

**Normally invisible** — the waste is recycled instantly:

```
  produce → [box1] → sent → acked → freed ✓
            [box2] → sent → acked → freed ✓
  Alive at any moment: 1–2 boxes.   Memory: flat.
```

**Under a stall, one stuck record holds everything hostage.** Boxes are freed
strictly in order:

```
  [box    1] ✗ retrying against the dead broker ── BLOCKED
  [box    2] ✓ delivered fine … but stuck behind box 1
  [box    3] ✓ delivered fine … but stuck
       ⋮
  [box 9600] ✓ delivered fine … but stuck

  9,600 = delivery.timeout.ms (120 s) × 80 msg/s
  9,600 × 44 KB = 422 MB predicted / 425 MiB measured
```

Not 9,600 failures — *one* failure, and 9,599 successes trapped behind it.

**Then the heap can't shrink**, because glibc only returns memory from the top:

```
  before stall   [██░░░░░░░░░░░░░░░░░░░░░░]   24 MB   (██ = in use)
  during stall   [████████████████████████]   425 MB
  after freeing  [██░░░░░░░░░░░░░░░░░░░█░░]
                     └─ 400 MB free ─┘  ▲
                                        └ one live object blocks
                                          everything below it
```

**And the plateau is self-limiting** — which is why the graph shows one step,
not a climb:

```
 790 ┤        ╭╮
     │        ││           ╭╮    ╭╮
 240 ┤────────╯╰───────────╯╰────╯╰──────
     │   ↑    └─ first spike forced the heap to 790 MB;
 170 ┤───╯       glibc trimmed back only to 240
     └───────────────────────────────────────
             later spikes REUSE those 240 MB —
             the heap never has to grow again
```

**What the fix changes — measured, not predicted:**

| | before | after |
|---|---|---|
| peak RSS, 10 KB payload, stalled broker | 449.4 MiB | **246.8 MiB** (−45%) |
| bytes/block at the `send` call site | 44,032 | **191** |

The bookkeeping is 99% eliminated. The reduction is 45% rather than the ~79%
first predicted, because removing the 43 KiB node **unmasked a second cost**:
roughly **10 KiB of native RSS per outstanding record, independent of payload
size**, invisible to tracemalloc and originating on the Rust side. That is now
the dominant per-record term and is a separate open investigation.

Note also that the fix does **not** meaningfully reduce the plateau (241 MiB
after, vs 449 before the spike is even considered) — see the allocator section
below. Peak and plateau turn out to be independent problems.

## Why a roll triggers it

Nodes are freed strictly in order, and only once every future in a node has
resolved. When a broker stops answering, one record retrying against it **pins
every node behind it — including records already delivered successfully.**

Normal operation never shows this: nodes are created and freed continuously, so
only a handful exist at once. A stall converts a steady trickle into thousands
of retained 44 KiB structs.

## Why the existing safeguard didn't help

There is a backpressure gate at 1,000 records — but it counts
`accumulated_records`, which **is reset to zero on every drain**
(`_confluentkafka.c:436`). It bounds the pre-drain staging queue, not the
outstanding-record list. So there is currently no effective ceiling on memory
when a broker stalls.

This also explains an earlier puzzle: we saw `producer.outq` reach 6,536 despite
a "1,000 limit". The two counters measure different things.

## Whose bug is it?

**The Python bindings' C extension.** Not the Rust client, and not the soak
script.

We confirmed this by measurement rather than inference:

| hypothesis | test | result |
|---|---|---|
| Consumer fetch buffers (`fetch.max.bytes=50MB`) | consumer-only, 170 MB backlog, brokers hung mid-drain | peaked **+36 MiB**, flat — refuted |
| Payload size drives it | shrank payload 200× (10 KB → 50 B) | `BatchNode` term **unchanged** (339 vs 341 MiB) — it's the struct, not the data |
| Rust client leak | tracemalloc snapshot at peak | all of it attributable to the `Producer_send` call site in the extension |

## Is it a leak? And why doesn't memory come back after the roll?

**It is not a leak. The records really are freed — but `free()` does not return
memory to the operating system.** This is the part that confuses everyone, so
step by step:

**1. The nodes are genuinely freed.** Once the stalled broker recovers, every
future resolves and all 8,132 nodes are released. Nothing is lost: at that
point only **0.63 MiB** was still live in Python, while RSS sat at **404 MiB**.

**2. `free()` hands memory back to the allocator, not the kernel.** glibc keeps
it on a free list so the process can reuse it. RSS measures pages the process
still has *mapped*, so it doesn't move.

**3. The cause is interleaving fragmentation, not the allocation size.** An
earlier draft of this document blamed glibc's 128 KiB `M_MMAP_THRESHOLD` —
claiming 44 KiB fell just below it and so was never returned. **That was wrong,
and measurement refuted it:** in isolation, default glibc returns 9,400 × 44 KiB
allocations *in full* (405 MB → 1.4 MB), and forcing
`MALLOC_MMAP_THRESHOLD_=32768` moved the plateau only 449 → 359 MiB.

What actually traps the memory is that the nodes are **interleaved** with 10 KB
payload buffers and Python objects. Freeing the nodes leaves holes that cannot
be coalesced into a returnable region, because live allocations sit between
them.

**4. glibc only shrinks the heap from the top.** The soak never stops
allocating, so newer live objects sit *above* the freed region. Free space
trapped beneath a live allocation cannot be returned, however much of it there
is. That is fragmentation, not a leak.

> The car-park version: everyone drove home and the spaces are empty, but the
> car park is still the same size — and you cannot sell off the back half while
> one car is still parked at the far end.

**The proof.** After a full drain and 60 s of idle, RSS was still 404.2 MiB.
Calling `malloc_trim(0)` — which forces glibc to walk its arenas and hand free
pages back — released **301.6 MiB** immediately. The memory had been free the
whole time. Nothing in normal operation ever calls it.

**Why the baseline ratchets.** Each roll grows the heap a little and leaves a
little more fragmentation behind. Even after the trim, 102.6 MiB remained
against a 24.5 MiB baseline.

Note the ratchet is **sub-linear**: spikes of 790, 350, 690, 680 and 330 MiB
moved the baseline only from ~170 to ~240 MiB, because each spike largely reuses
the space the previous one freed. This is retention, not runaway growth — the
process is not on a straight path to OOM. But the retained footprint is real,
and on a host running four soaks it is real four times over.

## One operational finding worth knowing

**A graceful broker roll reproduces none of this.** `docker stop` moves
leadership before closing the socket: RSS went 52 → 54 MiB, `outq` never
exceeded 2. Reproduction required `docker pause` — a broker that stops answering
with the connection still open.

Which implies the production rolls are **not** landing gracefully, or something
else is severing connectivity mid-flight. Worth investigating independently of
this bug.

## The fix

**Size `BatchNode`'s arrays to actual occupancy** instead of a fixed 1,100. A
node holding one record then costs ~100 bytes instead of 43 KiB. No semantic
change, C-extension only.

Expected: **~79% reduction at 10 KB payloads** (~620 MiB → ~130 MiB), ~97% at
50 B.

**It does not eliminate the spike.** The payload bytes themselves are still
pinned by the in-order drain. Removing that needs a second, deeper change to
complete records out of order — a real change to the bindings' completion model,
which deserves its own review.

Two follow-ups that need design discussion rather than a patch:

1. The backpressure gate should count *outstanding* records, not staged ones, if
   it is meant to mirror Java's `buffer.memory` blocking. As written it cannot.
2. Head-of-line blocking in the pending drain is structural, and is the
   amplifier that turns one stalled record into thousands of retained nodes.

## Peak and plateau are separate problems

Measured across the same reproduction (10 KB payload, paused broker):

| variant | peak | after 60 s idle |
|---|---|---|
| glibc, pre-fix | 449.4 MiB | 449.4 MiB |
| jemalloc, pre-fix | 447.9 MiB | **108.8 MiB** |
| Fix A, glibc | **246.8 MiB** | 241.2 MiB |
| Fix A + jemalloc | 257.2 MiB | **115.9 MiB** |

- **Peak is live memory.** Only the allocation fix moves it (449 → 247). No
  allocator can shrink memory that is genuinely in use.
- **Plateau is retention.** Only the allocator moves it. jemalloc's decay-based
  purging (`dirty_decay_ms`, ~10 s) returns it with no forced trim; Fix A
  barely helps, because the payloads still fragment the arena.

No measurable throughput cost for any variant. Two negative results:
`MALLOC_ARENA_MAX=2` measured **27% worse** (570 MiB peak), and
`MALLOC_MMAP_THRESHOLD_` gave only 449 → 359 MiB.

**A caution if jemalloc is adopted.** Pre-fix under jemalloc, the plateau is
108.8 MiB *with the defect fully intact* — a healthy-looking graph over an
unfixed bug. That argues for shipping jemalloc to **users**, where the benefit
is real, while keeping at least one glibc soak arm so the instrument still shows
what it is meant to detect.

## Takeaway

The soak did its job. Nine hours into the first real batch it surfaced a
genuine memory defect in a shipped code path — one that only appears when a
broker stalls without closing its connections, which no unit or integration
test covers.
