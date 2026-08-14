# Investigation: `python-soak-ht` RSS spikes during broker rolls

**Status:** diagnosis only. Nothing has been changed. Reviewed-before-fix, per the
investigation brief.

**Date:** 2026-08-14
**Branch:** `dev/python-soak-client` @ `7d65a793` (post-rebase; Rust client and
bindings rebuilt from this HEAD for every measurement below)

---

## 1. Verdict

The spikes are caused by the **Python C extension**
(`bindings/python/_confluentkafka.c`), not by the Rust client and not by the soak
script.

Two defects compound:

1. **`BatchNode` is a fixed 44,016-byte (43.0 KiB) struct that is allocated per
   *drain cycle*, which at soak rates means roughly one per record** — five
   parallel `[1100]` pointer arrays (`producer_records`, `producer_structs`,
   `complete_cbs`, `futures`, `batch_errors`) are allocated in full whether the
   node ends up holding 1 record or 1100. At 80 msg/s against a 10 ms drain loop,
   ~1.16 records share each node. Measured: **43 KiB of bookkeeping per
   outstanding 10 KiB record — 4.3x the payload it describes.**

2. **The pending-batch list that holds those nodes is unbounded and drains
   strictly in order, so one slow record pins every node behind it.**
   `Producer_poll_futures_thread` (`_confluentkafka.c:342`) processes one
   `BatchNode` at a time, blocking in `FutureRecordMetadata_get_all` on that
   node's futures, and only calls `PyMem_RawFree(batch_to_free)` after the whole
   node completes. A single record retrying to an unresponsive broker (up to
   `delivery.timeout.ms`, default 120 s) therefore holds **every** later node
   resident — including nodes whose records were acknowledged long ago.

The advertised backpressure does not stop this. `PRODUCER_RECORD_SLOT_THRESHOLD`
= 1000 bounds only `accumulated_records`, the *pre-drain staging* counter, and
the send task resets it to 0 on every drain (`_confluentkafka.c:436`) — roughly
every 10 ms. So `producer.send()` never blocks, and the number of outstanding
records is bounded by nothing.

This also resolves the open question from the earlier local test: `outq` reaching
**6536 against a "1000-record" bound** is not a contradiction. `self.outstanding`
counts sent-but-undelivered records; `accumulated_records` counts records staged
for the next 10 ms drain. They measure different things, and only the latter is
capped.

The non-returning baseline is **glibc allocator retention, not a leak** — proven
in §5.

---

## 2. Reproduction

3-broker KRaft cluster (`apache/kafka:4.2.0`, RF=3, 6 partitions), soak run with
`HI=true`, `SOAK_PARTITIONS=6`, rate 80 msg/s, 10240 B payload.

| Scenario | Baseline | Peak | Settles at |
|---|---|---|---|
| Steady state | 52 MiB | — | 52 MiB |
| **Graceful roll**, all 3 brokers one at a time | 52 MiB | 54 MiB | 54 MiB |
| One broker hung 90 s (`docker pause`) | 54 MiB | 95 MiB | **96 MiB** |
| Two brokers hung 150 s | 96 MiB | 425 MiB | **425 MiB** |

Two things to take from this table.

**A graceful roll does not reproduce it.** Controlled shutdown moves leadership
before the socket closes, the producer retries to the new leader immediately, and
`outq` never exceeded 2. This is worth knowing: if production rolls are graceful,
the roll is a *correlate*, not the cause — the trigger is a broker that stops
answering while the connection stays open (frozen pod, node pressure, long GC, or
a roll moving faster than ISR recovers, which makes `acks=all` retry).

**The baseline ratchets exactly as reported.** 52 → 96 → 425 MiB, each spike
leaving a permanently higher floor, matching the Grafana shape.

---

## 3. Python vs native attribution

`tracemalloc` tracked RSS almost 1:1 throughout. That is initially
counter-intuitive — but it is the correct reading here, because the C extension
allocates via `PyMem_RawMalloc`, which **is** a tracemalloc-traced domain. So
"tracemalloc is climbing" does *not* mean "Python objects"; it means the traced
allocator, which includes the extension.

The top-sites snapshot at peak makes the attribution unambiguous. Producer-only
process, 150 s stall, 9436 outstanding records:

```
341.4 MiB   8132 blocks   /w/bindings/python/producer.py:258     <- Producer_send -> BatchNode
 92.4 MiB   9436 blocks   payload bytes (one per outstanding record)
  6.8 MiB  18876 blocks   threading.py:258
  1.7 MiB  16193 blocks   producer.py:230
```

`341.4 MiB / 8132 blocks = 44,032 bytes per block`. `sizeof(BatchNode)` compiled
on this platform is **44,016 bytes**. That is the struct, one block per node, and
it is the single largest consumer of memory in the process by a factor of 3.7
over the actual message payload.

---

## 4. Which component — the bisect

All runs: producer only, no consumer in the process, 150 s stall, 80 msg/s.

| | HI (10240 B, lz4) | non-HI (50 B, no compression) |
|---|---|---|
| Baseline RSS | 24.5 MiB | 24.5 MiB |
| Peak RSS | 422.5 MiB | 311 MiB |
| Peak tracemalloc | 452.6 MiB | 352.8 MiB |
| **`BatchNode` (producer.py:258)** | **341.4 MiB / 8132 blocks** | **339.3 MiB / 8084 blocks** |
| Payload bytes | 92.4 MiB | 0.7 MiB |
| RSS per outstanding record | ~40 KiB | ~30 KiB |

Shrinking the payload by 200x barely moves the spike: the `BatchNode` term is
**identical** (339 vs 341 MiB). The 10 KB payload contributes ~21% of the HI
spike; the fixed 43 KiB struct contributes the rest.

**The HI fetch tuning is not implicated, and I tested it at its strongest.** A
consumer-only process with `fetch.max.bytes=52428800` and
`max.partition.fetch.bytes=10485760` draining a 17,365-record (~170 MB) backlog,
with both brokers hung for 60 s mid-drain:

```
baseline 25.8 MiB -> peak 62.1 MiB (+36 MiB), flat across the outage, no growth after
```

`fetch.max.bytes` is a ceiling on what a broker may return, not a preallocation,
and the fetch path is already bounded correctly on the Rust side: the
buffered-partition and pending-node exclusions in
`src/consumer/internals/abstract_fetch.rs` (lines ~690, ~707-711, ~820) keep it
to one in-flight plus one buffered fetch per node, and `FetchBuffer::retain_all`
is called on every assignment change. **The leading hypothesis is refuted.**

The Rust producer is also not implicated: `buffer.memory` (32 MiB) and the
`BufferPool` bound the accumulator correctly. The reason that bound does not
bound *record count* is that `MemoryRecordsBuilder` compresses as it appends, and
the soak's payload (`b" SoakRecord nr #0"` repeated) is near-perfectly
compressible — so 32 MiB of accumulator holds effectively unlimited records. That
is correct Java-faithful behaviour; it just means the accumulator provides no
backstop for the C extension's unbounded pending list.

---

## 5. Leak or allocator retention?

**Allocator retention.** Decisive evidence from the HI run, after the stall ended
and every record drained:

```
# drained: outstanding=0  rss=404.2 MiB
# idle+10s ... idle+60s   rss=404.2 MiB   tracemalloc=0.63 MiB
# malloc_trim() -> 1 ; rss 404.2 -> 102.6 MiB (released 301.6 MiB)
# baseline was 24.5 MiB
```

`tracemalloc` fell to 0.63 MiB — the application freed everything — while RSS sat
at 404.2 MiB through 60 s of idle. `malloc_trim(0)` then handed 301.6 MiB back to
the kernel. Nothing is leaked; glibc is holding freed 43 KiB chunks. They sit
below the 128 KiB `M_MMAP_THRESHOLD`, so they come from the heap/arenas rather
than `mmap`, and glibc can only return contiguous free space at the top of a
heap. The same result in the non-HI run (310.3 → 136.4 MiB).

Note the trim did **not** return to baseline (102.6 and 136.4 MiB vs 24.5 MiB).
Residual fragmentation survives even an explicit trim — and production never
calls one. That is the ratchet.

So the graph is alarming but the process is not on a path to OOM from a leak. It
*is* a real defect: a stalled broker inflates RSS by ~40 KiB per in-flight record
and the memory is never given back.

---

## 6. Honest gap

I reproduced ~398 MiB of growth (24.5 → 422.5 MiB) against production's ~620 MiB
(170 → 790 MiB). The *shape* matches exactly — linear in outstanding records at
~40 KiB each, non-returning baseline, repeated spikes ratcheting — but the
magnitude is ~65% of production's largest spike.

At 80 msg/s and `delivery.timeout.ms=120000`, a single stalled batch can pin at
most ~9,600 records ≈ 400 MiB, which is what I measured. Production's 620 MiB
implies ~15,000 pinned records. Candidate explanations, in rough order of
likelihood, none yet tested:

- Overlapping stalls: a multi-broker roll re-stalls the head of the queue before
  it drains, so nodes from successive outages coexist.
- Production message rate or partition count differs from the 80 msg/s / 6
  partitions I ran.
- Greater glibc arena inflation under production's thread count.
- OTEL exporter buffering during the outage (my repro ran JSONL-only).

**What would settle it:** the production `producer.outq` series at the spike. If
it peaked near 15,000, the mechanism is fully confirmed and only the stall
duration differs. That series is already emitted — `set_gauge("producer.outq",
outstanding)` in `soakclient.py:1856` — so it should be in Grafana alongside
`memory.rss`. Overlaying the two is a five-minute check that closes this gap
without another soak run.

---

## 7. Proposed direction (NOT applied)

Flagging one change that looks safe and clearly correct, per the brief's
"propose, don't apply". It touches only the C extension, not the Rust send path
and not the fetch tuning, so it does not affect the running batch's client
behaviour:

**Make `BatchNode` hold its arrays out-of-line, sized to actual occupancy**, or
equivalently reduce `PRODUCER_RECORD_SLOT_CAPACITY` and allocate the node's
arrays after the drain count is known. A node holding 1 record would then cost
~100 bytes instead of 43 KiB, cutting the spike by ~97% at 50 B payloads and
~79% at 10 KB — without changing any semantics.

Two further items that need design discussion rather than a patch, because they
change blocking behaviour:

- The backpressure gate should count *outstanding* records, not staged ones, if
  it is meant to do what its comment claims ("mirrors Java's `send()` blocking
  once `buffer.memory` is full"). As written it cannot.
- The strictly-ordered pending-batch drain means head-of-line blocking is
  structural. Completing nodes out of order, or per-record rather than per-node,
  would remove the amplification where one stalled record pins thousands of
  delivered ones.

I have deliberately not written any of this. Recommend reviewing the diagnosis
first.

---

## 8. Reproducing this

Artifacts are in the session scratchpad (`compose.yml`, `exp_stall.py`,
`exp_consumer.py`, `run_exp.sh`, `sampler.py`) and the raw series in
`out/sample-soak-roll.csv`, `out/exp-hi.csv`, `out/exp-nohi.csv`. The method:

1. 3-broker KRaft cluster, RF=3, 6 partitions.
2. `HI=true` soak against it; let RSS settle.
3. `docker pause` one or two brokers — **not** `docker stop`; a graceful stop is
   absorbed and reproduces nothing.
4. Sample RSS, `tracemalloc`, and `outq` together; take a
   `tracemalloc.take_snapshot()` at peak outstanding.
5. After full recovery, call `malloc_trim(0)` via `ctypes` to separate retention
   from a leak.

---

# Part 2: Fix A implemented and measured

**Commit:** `e2350f86` on **`fix/python-binding-batchnode-memory`** (branched from
`origin/master` `e0f8165e`, not from this soak branch). Verified on a local
throwaway merge of that branch with this one; that merge was not pushed.

## 9. What changed

`BatchNode`'s five per-record arrays moved out-of-line into one allocation
carved into pointer-sized sub-arrays, starting at 8 slots and doubling to the
**unchanged** `PRODUCER_RECORD_SLOT_CAPACITY` chaining boundary. A node holding
one record costs ~392 bytes instead of 44,016.

Node boundaries, drain ordering, completion granularity, the
`PRODUCER_RECORD_SLOT_THRESHOLD` backpressure gate and every public surface are
untouched. Growth allocates a new block and relocates the live entries rather
than reallocating, because changing the capacity moves every sub-array's offset;
it runs at most a handful of times per node and never at soak rates. Allocation
failure now raises `MemoryError` with the record's references released, where
the previous unchecked `malloc` would have dereferenced NULL.

## 10. Results

Controlled producer-only stall (`exp_stall.py`, 10 KB lz4, 150 s two-broker
`docker pause`, ~9,390 outstanding at peak). **Both arms were re-measured on the
same master-based Rust build**, so the diagnosis-phase numbers in §4 are not
reused for the comparison.

| | before | after |
|---|---|---|
| Peak RSS, 10 KB payload | 449.4 MiB | **246.8 MiB** (-45%) |
| Peak RSS, 50 B payload | 327.9 MiB | **151.8 MiB** (-54%) |
| tracemalloc bytes/block at `producer.py:258` | 44,032 | **191** |
| `producer.py:258` total at peak | 379.9 MiB | **3.3 MiB** (-99%) |
| RSS after `malloc_trim(0)` vs baseline | 123.8 vs 24.5 MiB | **103.7 vs 24.4 MiB** |

Per-record marginal cost, least-squares over the rising phase only (the earlier
endpoint arithmetic conflated a fixed offset with the slope):

| | before | after |
|---|---|---|
| RSS per outstanding record, 10 KB | 41.17 KiB | **22.40 KiB** |
| tracemalloc per outstanding record, 10 KB | 53.40 KiB | **12.40 KiB** |
| RSS per outstanding record, 50 B | 31.00 KiB | **12.54 KiB** |
| tracemalloc per outstanding record, 50 B | 43.47 KiB | **2.50 KiB** |

Full soak, `HI=true`, 6 partitions, same hang: peak RSS **393 -> 209 MiB**.

## 11. Success criteria — one is not met

- **">=70% peak-RSS reduction at 10 KB": NOT MET. 45%** (47.7% of growth above
  baseline). Reported as measured rather than reframed.

  The bookkeeping the fix targets is gone — 99% of it, 53.40 -> 12.40 KiB traced
  per record. What remains is memory the fix does not touch: the 10 KiB payload
  itself, plus **~10 KiB per outstanding record of native RSS that tracemalloc
  does not see**. That residue is payload-size-independent (12.54 KiB/record RSS
  against 2.50 KiB traced at a 50 B payload), so it is not a payload copy — it
  is per-record allocation on the Rust side, invisible before because the 43 KiB
  `BatchNode` dominated. That is a new finding and a candidate for the next
  slice; it is not something Fix A could have addressed.

- **"bytes/block drops to approximately the payload size": exceeded.**
  44,032 -> 191 bytes/block. The site now holds only the node and its slot
  block; the payload is allocated by the caller and was never at this site.

- **"Zero change to delivery semantics": met.** Both soak arms
  `verdict=PASS`, `missed=0`. Unpaced throughput 241k rec/s before vs 243k after
  (two runs each, ~3% run-to-run spread), CPU 5.3-5.4 ms per 1k records in every
  run. Recovery after unpause clean in both. The growth path is genuinely
  exercised here: at full tilt nodes fill to the 1000-record threshold, so every
  node walks 8 -> 16 -> ... -> 1024.

Test suites, all on the merged verification branch: Python binding unit tests
**78 passed / 2 skipped**; soak unit tests **130 passed**; C tests
`test_mock_producer` **25**, `test_mock_consumer` **19**, `test_kafka_producer`
**11**, all passing. The C tests do not reference `BatchNode` — it lives only in
`_confluentkafka.c`, which is Python-only — but they do exercise the Rust FFI
batch-send path the fix calls into, unchanged.

Incidental: a Rust producer error, `Can't find batch created for topic id ...`,
appears once during recovery in three of seven runs, **both with and without the
fix and both with and without an allocator change**. Pre-existing, unrelated to
this change, worth a separate look.

---

# Part 3: allocator experiments (no code change)

Same harness, same 10 KB / 150 s hang, ~9,390 outstanding at peak. Rows 1-4 use
the **pre-fix** extension so each allocator is measured against the original
defect; rows 5-6 add Fix A.

| # | variant | baseline | peak RSS | after drain | after 60 s idle | after `malloc_trim` |
|---|---|---|---|---|---|---|
| 1 | glibc default, pre-fix | 24.5 | 449.4 | 449.4 | 449.4 | 123.8 |
| 2 | `MALLOC_MMAP_THRESHOLD_=32768`, pre-fix | 24.5 | 415.1 | 359.0 | 359.0 | 120.7 |
| 3 | `MALLOC_ARENA_MAX=2`, pre-fix | 24.4 | **570.4** | 570.4 | 570.4 | 89.6 |
| 4 | **jemalloc** `LD_PRELOAD`, pre-fix | 30.0 | 447.9 | **108.8** | **108.8** | n/a |
| 5 | **Fix A**, glibc | 24.4 | **246.8** | 241.2 | 241.2 | 103.7 |
| 6 | **Fix A + jemalloc** | 30.1 | **257.2** | **115.9** | **115.9** | n/a |

All figures MiB. Under jemalloc, glibc's `malloc_trim` returns 0 and frees
nothing, which confirms jemalloc is actually in charge.

**The coordinator's question — does fixing the allocation make the allocator
choice moot? No.** The two are orthogonal, and the table separates them cleanly:

- **Peak is live memory.** Fix A takes it 449.4 -> 246.8. No allocator moves it:
  jemalloc peaks at 447.9, essentially identical to glibc's 449.4.
- **Plateau is allocator retention.** jemalloc takes it 449.4 -> 108.8 with no
  forced trim. Fix A barely touches it: 241.2 under glibc, still **216.8 MiB
  above baseline**. Fix A removes the allocations but the *interleaved* churn of
  payloads and Python objects still fragments the arena.

So Fix A alone does not fix the graph the operator is looking at, and jemalloc
alone does not fix the memory the process actually needs.

**jemalloc decay timing:** ~10 s, matching the default `dirty_decay_ms=10000`.
Measured twice in one run — 447.9 MiB at t=162 s down to 186 MiB by t=172 s, and
223.6 MiB at t=192 s down to 110.0 MiB by t=200 s.

**`MALLOC_MMAP_THRESHOLD_=32768` disappoints, and the reason is instructive.**
In an isolated C program, 9,400 x 44,016-byte allocations are returned in full on
free by *default* glibc (405 MB -> 1.4 MB) with no env var at all. So the plateau
was never "44 KB sits below the mmap threshold". It is fragmentation from the
interleaved, multi-threaded pattern: freed holes stranded between live payload
and Python objects, which glibc can only trim from the top of a heap. Pushing
just the `BatchNode`s to `mmap` leaves the 10 KiB payload allocations — which are
below any sane threshold — still creating the holes.

**`MALLOC_ARENA_MAX=2` was the worst option measured**, raising peak RSS to
570.4 MiB, 27% above default glibc. Single run, not repeated, so treat the
magnitude as indicative — but there is no evidence it helps, and some that it
hurts.

**Throughput cost: none measurable for any variant.** Unpaced 10 KB producer,
60 s, ~245k rec/s: glibc 251,152; `MALLOC_MMAP_THRESHOLD_` 245,263;
`MALLOC_ARENA_MAX=2` 245,462; jemalloc 246,264 rec/s. CPU 5.2-5.3 ms per 1k
records throughout. Run-to-run spread is ~3%, so all four are indistinguishable.
The feared `mmap`/`munmap` pair per node does not show up.

## 12. The instrumentation risk, stated plainly

Adopting jemalloc **on the soak host** would make the graph look healthy while
the underlying waste remained. That is the specific failure mode to avoid: the
bindings ship to users on glibc, so users would keep the behaviour while our
instrument stopped showing it. Row 4 is exactly that trap — a beautiful 108.8
MiB plateau with the 44 KB-per-record defect fully intact.

This argues for fixing the allocation (done, row 5) and treating the allocator
as a **separate, deliberate, shipped-with-the-product** decision, not a soak-host
tweak. If jemalloc is adopted it should be adopted for the users who have the
problem, and the soak should keep at least one glibc arm so the instrument still
reflects what users run.

Recommendation, for review not for action: rows 5 and 6 are both defensible.
Nothing further applied.
