# Consumer throughput bottleneck — investigation

Status: **2026-06-05 — READ "UPDATE 4" (bottom) for the proven diagnosis.
DEFINITIVELY READ-BOUND: socket Recv-Q sits at ~350 KB for Rust vs ~0 for Java
on the identical broker — the broker has data ready and our consumer reads it
too slowly (not broker, TCP, buffer, allocation, or protocol). Mechanism: the
Selector reads a readable channel in small chunks with heavy per-chunk async
overhead AND bounces back through run_once on wakeup-pokes. Fix = Java-NIO-style
tight drain loop. NOT yet implemented.**

## Question
At true-peak load the producer sustains ~234k msg/s (1 KB) but the Rust consumer
drains only ~134k msg/s (~131 MiB/s) on ~1.1 cores — it cannot keep up. Why?

## Method
`consumer-perf --peak` (1 KB, 8 partitions, `latest`), then a 20 s macOS `sample`
of the consumer process at steady state. The benchmark's own loop breakdown
reported **`poll().await` = 99.9 %, per-record processing = 0.1 %** — so the
ceiling is entirely inside the client's `poll()`, not the harness.

## Finding — it's allocation-bound
The hot path (app thread, inside `poll()`):

```
AsyncKafkaConsumer::poll
└─ FetchCollector::collect_fetch                         3040 samples
   └─ CompletedFetch::advance_to_next_fetched_record     2042
      └─ DefaultRecordBatch::iter_records
         └─ DefaultRecord::read_from_buffer → read_from_body → read_bytes → malloc
```

Aggregate over the 20 s sample:
- **malloc/free = 3877 samples ≈ 62 % of the 6296-sample poll-path on-CPU work.**
- The allocations come from the record decode copying every field:

`src/common/record/default_record.rs`:
```rust
fn read_bytes(buffer: &[u8], pos: &mut usize, size: i32) -> Result<Option<Vec<u8>>, _> {
    ...
    let data = buffer[*pos..*pos + size].to_vec();   // ← heap copy per key AND per value
    ...
}
struct DefaultRecord { key: Option<Vec<u8>>, value: Option<Vec<u8>>, headers: Vec<RecordHeader>, ... }
```

So for every record the client heap-allocates: 1 `Vec` for the key, 1 for the
value, 1 for headers (+ per-header allocs), plus the owned `DefaultRecord` and
the downstream `ConsumerRecord`. At ~134k rec/s that is ~400k+ mallocs/s — and
macOS's allocator (`szone_malloc`/`small_malloc`/`free_small`) is the single
biggest CPU consumer in the profile.

The user-supplied `Deserializer<T>` then receives a `&[u8]` that points into
those just-allocated `Vec`s — i.e. the copy already happened. Even a zero-cost
deserializer (the benchmark's length-only one) cannot avoid it.

## This violates the §27 zero-copy receive-path contract
`consumer-threading.md` §27 requires: *"fetched bytes are owned by one buffer in
`CompletedFetch`, every downstream type borrows slices from it, and the
`Deserializer<T>` trait takes `&[u8]`."* The decode layer instead **materializes
owned `Vec`s per field**, so the contract is not actually realized end-to-end.
(`CompletedFetch` already owns the buffer; the gap is `DefaultRecord` copying out
of it instead of borrowing.)

## Proposed fix (NOT applied — needs approval; sizeable)
Make the record decode **borrow** slices from the `CompletedFetch` buffer instead
of `to_vec()`-ing, and hand those `&[u8]` straight to the deserializer:

- `read_bytes` returns `Option<&[u8]>` (borrow, no copy); `DefaultRecord<'a>`
  holds `key: Option<&'a [u8]>`, `value: Option<&'a [u8]>` borrowing the batch
  buffer; `iter_records` / `advance_to_next_fetched_record` thread the buffer
  lifetime through.
- The `Deserializer<T>` already takes `&[u8]`; feed it the borrowed slice. Only
  the user's decoded `T` (and §27's owned headers) are allocated — which is the
  intended, unavoidable allocation.
- Headers: §27 already allows owned headers for Milestone 8; keep, or borrow
  later. The big win is key/value (the large fields).

Expected impact: removing ~60 % of poll-path CPU should roughly multiply the
drain ceiling (rough order ~2–3×), likely letting the consumer keep up with the
~234k/s peak on ~1 core. Must re-verify with the same `sample` + a peak run.

### Cheap confirmation experiment (optional, non-invasive)
Set a faster `#[global_allocator]` (e.g. `mimalloc`) in the `consumer-perf`
binary. Since it's process-wide it also speeds the client's per-record allocs,
so a throughput jump confirms the allocation diagnosis (it does NOT fix the root
cause — that's the zero-copy decode above).

## Caveat on the latency numbers from these runs
All `--peak` runs saturate (consumer 134k < producer 234k), so the e2e-latency
percentiles are backlog age, not steady-state latency. For real tail-latency,
run a fixed sustainable rate (`--throughput 100000`, no `--peak`). The throughput
ceiling and the allocation profile are the trustworthy outputs here.

---

## UPDATE 2026-06-05 — mimalloc refutes "allocation-bound"; it's pipeline-bound

Ran the cheap confirmation experiment: set `mimalloc` as the process-wide
`#[global_allocator]` in `consumer-perf` (covers the client's allocs too) and
re-ran the same peak test.

**Result: 134k → 141k msg/s (+5%).** A 3–4× faster allocator moved throughput
only ~5%. So **allocation is NOT the primary throughput limiter** — the earlier
"~2–3×" prediction was wrong. (mimalloc has since been reverted; the harness
measures the stock allocator.)

Why the 62%-malloc profile was misleading: `sample` counts every thread at every
tick, running *or parked*. Re-reading the same profile:
- **Main thread:** ~39% of ticks in the poll-closure decode/alloc work, ~61%
  **parked** in `block_on` waiting for the next batch.
- **All 12 tokio worker threads:** dominated by `park_internal` / `park_condvar`
  — i.e. **parked** most of the time.
- Process CPU is **~1.1 cores on a multi-core machine** → no thread is saturated.

So the malloc samples are 62% of a *minority* active slice; the consumer spends
most of its time **waiting**, not computing. The ~134–141k/s ceiling is a
**serial pipeline limit**: how fast records flow broker → bg fetch task →
`FetchBuffer` → app `poll()`, not CPU or allocation.

### Revised next step — investigate the fetch pipeline (not the allocator)
Likely levers (to be confirmed):
- **Fetch pipelining:** is more than one fetch kept in-flight per broker, or is
  it one round-trip at a time? Per-poll bg-task↔app handoff latency.
- **`max.poll.records` = 500** and **fetch sizing** (`fetch.min.bytes`,
  `max.partition.fetch.bytes`, `fetch.max.bytes`) — none currently exposed via
  `ConsumerConfig` builders; tuning may need them exposed.
- Poll cadence: ~285 polls/s × 471 rec/poll = ~134k; raising records/poll or
  shortening the cycle is where the headroom is.

The zero-copy decode fix is still worth doing for **CPU efficiency** (lower CPU
per record helps at higher loads and lowers cost), but mimalloc shows it will
**not** lift this particular ceiling much. The throughput fix is in the fetch
pipeline.

---

## UPDATE 2 (2026-06-05) — Java baseline proves it's a Rust CLIENT bug, not structural

The earlier "single-broker structural / Java-shared" framing was **WRONG**.
Ran Java consumers on the **same single broker, same static 3M-record backlog**
(`kafka-consumer-perf-test.sh`):

| consumer | overall msg/s | fetch-only msg/s |
|---|---|---|
| **Java classic** | **489,636** | ~1,017,000 |
| **Java KIP-848** (`group.protocol=consumer`) | **886,525** | ~932,000 |
| **Rust (ours)** | **138,963** | — |

Java — *including the identical KIP-848 protocol with the same `bufferedNodes`
skip* — is **~3.5–6.4× faster on the identical setup**. So the broker easily
feeds ~0.9–1M msg/s to a consumer on one broker; the ~134–139k ceiling is a
**Rust client implementation defect**, not the fetch-session design or the
single broker. (The `bufferedNodes`-skip behavior is real and Java-shared, but
it is NOT what caps us — Java has it too and is 6× faster.)

### Root cause: the Selector reads large responses one small chunk at a time
Proven: fetch round-trip is **~42 ms for an 8 MB response even with no producer
running** (static backlog) — i.e. ~190 MiB/s effective read, vs Java's
~900 MiB/s. The 42 ms is client-side, not broker/network (the producer saw
<8 ms latency).

Code path (`src/common/network/selector.rs`, `kafka_channel.rs`,
`network_receive.rs`):
- `attempt_read` calls `channel.read()` **once** per `poll_channel` pass, wrapped
  in `tokio::time::timeout(Duration::ZERO, …)` — a timer/`Sleep` allocation per
  read attempt.
- `channel.read()` → `NetworkReceive::read_from` does the body read as a **single
  `try_read`** of one socket-buffer-worth (~256 KiB–1 MiB), then returns.
- The transport `read()` does `stream.readable().await` (a waker registration)
  **before each** `try_read`.
- So an 8 MB fetch response is read in ~8–32 chunks, and each chunk pays:
  a `Sleep(0)` timer alloc + a `readable().await` waker round-trip + (between
  chunks, back in `Selector::poll`) a `collect_readiness_futures` Vec-of-boxed-
  futures allocation + a `select!`. That per-chunk async overhead — not the
  bytes — is the ~42 ms.

Java NIO reads in a tight loop within one `pollSelectionKeys`, draining the
socket fully with no per-chunk timer/waker/select overhead.

### Proposed fix (NOT yet applied)
Drain the socket in a tight loop per read, like Java NIO:
- In `NetworkReceive::read_from` (body phase), loop `try_read` into the remaining
  buffer until `WouldBlock` or the buffer is full — one call consumes everything
  currently available, not one chunk.
- Drop the `tokio::time::timeout(Duration::ZERO, …)` wrapper in `attempt_read`
  (use a direct non-blocking `try_read`; only `await readable()` when a read
  returns `WouldBlock`).
- Net effect: one `poll_channel` pass consumes the whole available response with
  a handful of `try_read` syscalls and zero per-chunk timer/waker/select
  allocations.

Expected: fetch RTT drops from ~42 ms toward a few ms → throughput should rise
several-fold (target: approach Java's ~0.9 M msg/s on this broker). Must
re-measure RTT + throughput after.

The per-record decode `to_vec` (UPDATE 1) is a *separate, smaller* CPU
inefficiency; worth fixing for CPU but not the throughput ceiling. The read-path
fix above is the throughput fix.

---

## UPDATE 3 (2026-06-05) — A/B DISPROVED the "wakeup fragments reads" hypothesis

Tested the hypothesis by gating the selector's wakeup-return so it would not
honor a poke mid-read. Results on the static-backlog drain:

- POLL-PROF (baseline): ~145 polls/s, **75% return via wakeup-poke**, ~1.9
  reads/poll. (Confirms reads ARE chopped by pokes.)
- A/B v1 (gate too coarse — empty receives counted as "in progress"): reads got
  FAST (ttfb ~0, drain ~30ms) but fetches **stalled to ~1/5s** → 1.7k msg/s.
  (`channel.read()` eagerly creates an empty `NetworkReceive`, so the gate
  suppressed the between-fetch wakeup that triggers the next fetch.)
- A/B v2 (gate = receive with `bytes_read>0`): throughput **59.7k — WORSE than
  the 139k baseline**, and reads were **still slow** (ttfb ~20ms, drain ~50ms,
  45 would-blocks). So suppressing mid-read pokes did NOT speed the reads.

**Conclusion: the wakeup-interruption is NOT the dominant cause.** (Reverted.)

### What the A/B *did* reveal — two separate costs, neither is the wakeup
The ~50 ms/fetch splits into:
1. **~20 ms time-to-first-byte** — the gap *between* fetches: finish read →
   return → run_once → send next fetch → wait for its first byte. A/B v1 hid
   this (continuous polling, ttfb≈0), confirming it's an inter-fetch gap, i.e.
   **no prefetch/overlap** (no fetch in flight while we drain). On a single
   broker the `bufferedNodes`-skip forbids overlap; Java avoids the gap because
   its pipeline keeps data flowing.
2. **~30 ms read-phase** — streaming the 8 MB (~230 MiB/s vs Java ~900) — a
   read-cadence / TCP-window / `timeout(Duration::ZERO)`-double-wait issue
   (45 would-block give-ups per receive, each via a `timeout(0)` + readiness
   round-trip).

### Open / next experiments (not yet run)
- **Read-phase:** replace the `timeout(Duration::ZERO, channel.read())` +
  one-`try_read`-per-pass with a tight `try_read` loop draining all available
  bytes per pass (only `await readable()` on `WouldBlock`). Measure if drain
  drops toward Java's.
- **Time-to-first-byte:** instrument the request *send* side — is the ~20 ms in
  writing/sending the fetch request, in run_once cadence, or genuinely broker?
- Both are still **hypotheses**; each needs its own controlled measurement.

Status: root cause is in the fetch I/O path (confirmed) but **not yet pinned to
a single fix**; the leading wakeup-fragmentation theory was tested and rejected.

---

## UPDATE 4 (2026-06-05) — PROVEN read-bound via Recv-Q (the real diagnosis)

tcpdump needs root (blocked), so used a non-root, decisive test: sample the
socket `Recv-Q` (bytes the broker delivered to our kernel buffer but the app
hasn't read) during a static-backlog drain, Rust vs Java, same broker.

| consumer | Recv-Q median | avg | p90 | max | samples==0 |
|---|---|---|---|---|---|
| Java | **0** | 921 B | 0 | 100 KB | 118/120 |
| Rust | **352 KB** | 298 KB | 580 KB | 587 KB | few |

**Java keeps the receive buffer empty** (reads bytes the instant they arrive →
broker-paced, ~757k msg/s). **Rust lets ~350–580 KB pile up unread** → the data
is *available* and we read it too slowly. **Definitively READ-BOUND on our side**
— not the broker, not TCP, not buffer size, not allocation, not the protocol.

### Why our reads are slow (mechanism)
Two compounding factors in the Selector read path:
1. **Reads are fragmented** — POLL-PROF showed `selector.poll()` returns ~75% of
   the time via a wakeup-poke after only ~1.9 reads, bouncing back through the
   full `run_once` loop (~ms each) before reading again. Data piles up during
   those round-trips.
2. **Heavy per-read async machinery** — each read pass does
   `tokio::time::timeout(Duration::ZERO, channel.read())` (a `Sleep(0)` timer),
   `channel.read()` does `stream.readable().await` (waker registration), and the
   no-progress wait rebuilds a `Vec<Box<dyn Future>>` of readiness futures + a
   `select!` — per chunk. A/B v1 (stay in the poll, no run_once round-trips)
   read at ~270 MiB/s — still ~3× slower than Java's ~800, so this per-chunk
   overhead is real on its own.

Java NIO drains a readable channel in a tight `read` loop inside one
`pollSelectionKeys`, with none of this.

### Fix direction (proven target now)
Make the Selector drain a **readable** channel **continuously with minimal
overhead** before yielding: when a read returns data, keep reading (tight
`try_read` loop) until `WouldBlock` *or the receive completes*, and do **not**
return to `run_once` on a wakeup while the socket still has data to read. This is
the Java `pollSelectionKeys` pattern. (Earlier A/B attempts failed for unrelated
reasons — v1 used an empty-receive gate that stalled fetches; v2 gated on
`bytes_read>0` but kept the heavy machinery. The fix must address BOTH the
fragmentation and the per-chunk overhead, and be re-validated with Recv-Q
dropping toward 0 and throughput toward Java's.)

---

## UPDATE 5 (2026-06-05) — Fix implemented and validated

The fix landed in three coordinated pieces:

1. **Non-blocking `try_read` on the transport.** `TransportLayer` gained
   `try_read(&mut [u8]) -> io::Result<usize>` + `supports_try_read() -> bool`
   (default: unsupported → fall back to async `read`). `PlaintextTransportLayer`
   overrides both: `try_read` delegates to `TcpStream::try_read` (a direct
   syscall, no readiness await), `supports_try_read() == true`.

2. **Tight drain in `NetworkReceive::read_from`.** On transports that support
   `try_read`, the payload phase loops `try_read` until the receive buffer is
   full (`break`) or the socket returns `WouldBlock` — draining all
   currently-available bytes in one pass with no per-chunk `readable().await` /
   `Sleep(0)` / `select!`. The size-header phase likewise uses `try_read`. SSL /
   mock transports keep the single async `read` path. `attempt_read` calls
   `channel.read().await` directly for `try_read` transports (no `timeout(0)`
   `Sleep`), and the no-progress `select!` does **not** break out on a
   wakeup-poke while any channel is mid-receive (`any_channel_mid_receive()`),
   so a receive is never fragmented across `run_once` round-trips.

3. **Yield on the immediate-return path.** Because the `try_read` read path
   performs no `.await` of its own, a `poll(0)` pass that makes no progress would
   return without ever yielding to the runtime. On a single-threaded runtime a
   caller that spin-loops `poll(0)` (e.g. the `EchoServer` selector tests, where
   the peer task shares the runtime) would starve its peers. The immediate /
   deadline-passed branch now does one `tokio::task::yield_now().await` before
   returning — once per poll, not per read chunk, negligible under load and
   harmless when there is work. (Caught by `test_normal_operation` /
   `test_large_message_sequence` stalling 5 s to idle-timeout; root-caused via
   stall instrumentation showing all channels `has_send=false recv_bytes=0`
   waiting on a starved EchoServer.)

### Validation (live broker, 1 KB records, 12 partitions)

- **Socket Recv-Q (consumer side): median 0** (min 0, max 256 KB, n=57),
  versus ~352 KB median before the fix. The read now keeps up — data no longer
  piles up unread in the kernel buffer. This is the direct proof the read-bound
  bottleneck is resolved.
- **Live --peak throughput: ~248–285k msg/s** sustained (interval rate), with
  Recv-Q at 0 — the consumer matches the single-broker producer peak (~280k/s
  for 1 KB). Before the fix it drained ~139k/s and fell behind (Recv-Q 352 KB).
- **Static backlog drain (--no-produce --offset-reset earliest): peak interval
  391k msg/s** before the (~2.7M-record) backlog was exhausted, vs the 139k
  baseline.
- Full lib suite green: **1706 passed, 0 failed**; clippy clean.

### Remaining gap to Java's ~886k (separate follow-up, not read-bound)
The read path is no longer the limiter. The remaining distance to Java's
KIP-848 static-drain number is **inter-fetch latency / lack of prefetch
overlap**: the client waits for one fetch's full drain before issuing the next
to a node, so there is no pipelining of the next fetch's network round-trip
behind the current fetch's decode. Closing this needs fetch prefetch/pipelining
(issue a fetch to a node as soon as its buffered data is consumed, not after the
whole response is drained), and is tracked as a distinct optimization, not part
of this read-bound fix.

---

## UPDATE 6 (2026-06-08) — ceiling re-measured after the latency fix

The steady-state latency fix (design/current/consumer-latency-findings.md UPDATE
part 2) keeps a fetch continuously in flight (`send_prefetches` before the
`await_wakeup` block) — exactly the "no prefetch overlap" change this doc flagged
as the remaining throughput follow-up. Re-measured the static-backlog ceiling on
a 12.7M-record backlog (1 KB, 12 partitions), `--no-produce --offset-reset
earliest`, 30 s sustained:

  **~370k msg/s sustained** (361 MiB/s), steady across all intervals
  (370/373/368/369/377k), CPU ~86% (under one core), RSS stable ~130–240 MB.

Progression on the same single broker: original read-bound **139k** → tight
`try_read` drain fix **~233k** → continuous-prefetch latency fix **~370k**
(2.65× over original). Java KIP-848 on the same broker: **886k**.

Still ~2.4× below Java, and CPU is only ~86% of one core — **not CPU-bound**, the
consumer still waits. Remaining gap (consumer-side; Java proves the broker serves
faster):
  1. **One fetch in flight per node.** Continuous prefetch keeps exactly one
     fetch outstanding per node; the next fetch is not sent until the current
     response is fully drained, so fetch N+1's network round-trip does not overlap
     fetch N's decode. Deeper pipelining (≥2 in-flight per node, or fetching while
     decoding) is the likely next lever.
  2. **Per-record `to_vec()` decode allocation** (the §27 zero-copy violation in
     `default_record.rs`). At 370k msg/s this allocation is a real CPU cost.
Both are separate follow-ups, not addressed here.

---

## UPDATE 7 (2026-06-08) — §27 zero-copy decode: the real ceiling lever (370k → 644k)

UPDATE 6's ~370k ceiling turned out NOT to be round-trip-bound. CPU profiling +
code read found the limiter: `CompletedFetch::load_next_batch` re-walked
`MemoryRecords::batches()` from index 0 on every call (once per batch → O(N²) over
a fetch), and `BatchIterator::next()` `to_vec`-copied each batch
(`memory_records.rs`), so the whole partition payload was copied many times over.
The first §27 pass (per-record borrow, UPDATE part-2 of the latency doc) removed
the per-*record* `to_vec` but left this per-*batch* O(N²) copy — which is why that
pass did NOT move the ceiling (a corrected diagnosis: the first profile's
`memmove` was real but not the per-record copy; it was this batch walk).

Fix (commit c03e1d4, building on 395ca2f/46fcf95):
  1. **Incremental cursor** — `BatchCursor` tracks the next batch's absolute byte
     offset and advances it per batch; locating the next batch is O(1), not
     O(start_pos). No more re-walk.
  2. **Borrowing batch-header parse** — new `DefaultRecordBatchRef<'a>` reads batch
     header fields from a `&[u8]` slice of the buffer; no `to_vec` per batch.
     Uncompressed records section is `RecordSource::Borrowed(range)` into the
     buffer; compressed decompresses once per batch (§27-permitted). Owned
     `DefaultRecordBatch` accessors now delegate to the ref (single source of
     truth); the owned `BatchIterator` API is untouched for producer callers.
  3. **Clone eliminated** — `ensure_cursor` now *moves* `partition_data.records`
     into `MemoryRecords` (was a clone), fully satisfying §27 "one buffer".

### Measured (default config, 1 KB, 12 partitions, uncompressed backlog)
- **Static-backlog ceiling: ~370k → ~644k msg/s** (629 MiB/s), sustained
  (621/673/637/662/656k across intervals); **CPU dropped 85% → ~75%** (much less
  work per record). 1.74× over UPDATE 6; now ~73% of Java's 886k (was ~42%).
- Latency @ 100k/s: p99 12 → **9 ms** (no regression — slightly better), CPU
  47% → **43%**. Idle CPU @ 1k/s: ~5% (unchanged). Receive-path allocation
  budget: **2.15 allocs/record**.
- Full lib suite **1715 passed**; clippy + format clean. New
  `test_multi_batch_ordering_and_offsets` covers the ≥3-batch path end-to-end
  (uncompressed + gzip).

Ceiling progression: 139k (read-bound) → 233k (try_read drain) → 370k (continuous
prefetch) → **644k (zero-copy batch loading)**. Java KIP-848: 886k.

### Remaining gap to ~886k
CPU is now ~75% of one core (not pegged) — the remaining ~25% is the
single-fetch-in-flight-per-node round-trip (no overlap of fetch N+1's network RTT
with fetch N's decode). Deeper fetch pipelining (≥2 in-flight per node) is the
next lever; separate follow-up.
