# Consumer client comparison — Rust vs Java vs librdkafka

Collated results, June 2026. Single consumer, KIP-848 (`group.protocol=consumer`),
1 KiB records, `auto.offset.reset=latest` (live e2e latency = `consume_time −
record.timestamp`). CPU is **% of one core** (>100% = multiple cores; consumers are
multi-threaded). All clients keep up with the offered rate unless noted.

> **Read CPU within a table, not across tables** — each environment differs
> (partitions, plaintext vs TLS, 1 vs 16 cores, sampler). The cross-environment CPU
> *ratios* are the durable signal.

---

## Table 0 — LATEST CONSOLIDATED (post Phases 21–25, the current numbers)

Cloud `lkc-devcpgrq9do`, in-region EC2 (Graviton), **SASL_SSL**, KIP-848, 200k msg/s,
24 partitions, same `/proc` CPU method, ~3–5 min runs. Rust = Phases 21–25 + LTO +
`target-cpu=native`. **This supersedes the older cloud tables below.**

**Latency-tuned (default fetch), all three clients:**

| Client | Throughput | CPU (1 core) | p50 | p99 | p99.9 | RSS |
|---|---|---|---|---|---|---|
| **librdkafka-C** (stock) | 200k | **53.3%** | 4 | 10 | 26 | ~30 MB |
| **Rust** (default allocator) | 200k | **82.6%** | 4 | 10 | 34 | **~28 MB** |
| **Java** | 200k | **88.5%** | 4 | 11 | 34 | JVM heap † |

**Rust config / deploy variants:**

| Variant | Throughput | CPU | p50 | p99 | p99.9 | RSS |
|---|---|---|---|---|---|---|
| Rust + mimalloc (deploy opt) | 200k | 75.3% | 4 | 13 | 36 | ~50 MB |
| Rust, 64 KB batched (+mimalloc) | 200k | 47.4% | 6 | 23 | 29 | ~57 MB |

latency in ms. † Java "RSS" = JVM heap-used (GC sawtooth), NOT process-RSS comparable
(the Java harness sampled CPU + latency only).

**Takeaways:** throughput tied; **p50 = 4 ms** across all three; tails in the same band
(p99 10–11). **CPU: librdkafka 53.3% < Rust 82.6% < Java 88.5%** — Rust **beats Java**,
**~1.55× librdkafka**. **RSS: Rust (~28 MB) ≤ librdkafka (~30 MB).** mimalloc is an
opt-in deployment lever (−7 pp CPU for ~2× RSS); 64 KB batching trades ~2 ms p99 for a
large CPU drop (47.4%). Journey: cloud default-config CPU went **~130% → 82.6%** via
Phase 21 (dedicated bg thread), 22 (FxHash selector + lookup-once), 23 (persistent
readiness), 24 (ready-set sweep), 25 (FxHash fetch maps), + LTO. The remaining gap to
librdkafka is the single-bg-thread (Java `§10`) + tokio-runtime architecture — not
closable without ceasing to be a faithful Java translation. Flamegraph:
`design/current/cpu-flamegraph-phase25.svg`.

### Phase 26/27 update (2026-06-11, cluster lkc-devcnvw72vz, same method, 150 s windows)

Same-day, same-cluster A/B (older cluster above was deleted; cross-cluster CPU
compares are ±2 pp):

| Client | Throughput | CPU | wakeups/s | µs CPU/wakeup | p50 | p99 | p99.9 | RSS |
|---|---|---|---|---|---|---|---|---|
| **Rust Phase-26** | 200k | 83.2% | 6,849 | ~121 | 4 | 12 | 36 | 36 MB |
| **Rust Phase-27** | 200k | **76.4%** | 7,861 | ~97 | 4 | 13 | 74† | 29 MB |
| **librdkafka-C** (stock) | 200k | **52.0%** | 10,232 | ~51 | 4 | 9 | 25 | 25 MB |

† single-run tail noise (other intervals 23–34 ms).

**Phase 27 (hot-path sync/clone churn): 83.2 → 76.4% (−6.8 pp)**, latency and
throughput unchanged, RSS −7 MB. The fresh line-table profile (perf annotate
piercing fat-LTO inlining) overturned the "remaining gap is structural" framing
above — a large slice was synchronization + clone churn Java gets for free:
~68% of `prepare_fetch_requests` self-time was Arc-refcount + mutex futex ops
(2–3 `SubscriptionState` locks **per partition** per call, `FetchPosition`/
`Node`/topic-ids-map clones per call), `run_once` re-locked the bg-exclusive
delegate `tokio::Mutex` 3–4× per iteration, and the NetworkClient maps
(`in_flight_requests`, `connection_states`, `ApiVersions`) still hashed
String keys with SipHash per lookup (Java's `String` caches its hashCode).
Gap to librdkafka now **1.47×** (was 1.60×). Per-wakeup cost ~97 µs vs
librdkafka's ~51 µs (~1.9×, was ~6× pre-Phase-23). Remaining decomposition at
76.4%: tokio readiness machinery ~7% (→ mio `selectedKeys()`-style lever,
needs §8 sign-off), inherent (AES + kernel copy + syscalls) ~12%, memcpy
~4.5% (→ rustls `UnbufferedConnection`), allocator ~6.5% + memset 1.4%
(→ receive-buffer pooling / `Bytes`, deferred), app-side record decode ~5%.

### Phase 28 matrix (2026-06-11, same cluster/method)

Phase 28 (run_once scratch reuse + one request-managers lock + Java poll
order; zero-free appending receive reads) validated across four regimes —
**CPU-neutral, zero regressions; kept as allocation hygiene + poll-order
fidelity**:

| Arm | CPU | RSS | latency | Reference |
|---|---|---|---|---|
| A 200k/1KB latency-tuned | 76.2% | 30 MB | p50=4 p99=16 | P27 76.4% (neutral) |
| B 200k 64KB-batch | 46.2% | 25 MB | — | best default-allocator 64 KB number |
| C big-batch 2KB/200p/4MB | 41.7% | 101 MB | p50≈225 ms (4MB-fill profile) | 1-hr baseline 41.6%/105 MB (tied) |
| D 5k low-rate | 2.9% | 19 MB | p50=5 p99=8 | no idle/busy-spin regression |

Lessons: (a) the `vec![0u8; n]` memset was cheaper than its symbol share —
large receive buffers come from fresh mmap pages where zeroing is ~free; the
`memset` symbol is mostly rustls-internal. (b) **Phase 29 (rustls
`UnbufferedConnection`) was INVALIDATED before implementation**: rustls
0.23.38's unbuffered API does not decrypt in place (`ReadTraffic::next_record`
pops owned plaintext chunks from the same internal buffer as buffered
`reader().read()`; in-place decryption is an upstream TODO). The per-record
intermediate plaintext chunk (alloc + copy) is rustls's floor in every API it
offers — that is the structural delta vs Java `SSLEngine.unwrap` (~3-4 pp).
(c) **`check.crcs` asymmetry — MEASURED, and it reframes the headline**:
Java (and this port, faithfully) default `check.crcs=true`; **librdkafka
defaults `check.crcs=false`**. Re-measured librdkafka with
`check.crcs=true` (same cluster/method/window, 200k latency-tuned):

| Client @200k latency-tuned | CRC on (Java semantics) | CRC off | CRC cost |
|---|---|---|---|
| **librdkafka-C** | **66.7%** (p50=4 p99=10, RSS 33 MB) | 52.0% | **+14.7 pp** |
| **Rust (P27/28)** | **76.2%** (p50=4 p99=16, RSS 30 MB) | **74.5%** (p50=5 p99=12) | **+1.7 pp** |

### Phase 30 (2026-06-12, per-channel wakers — selectedKeys() on tokio)

Replaced the per-WAIT readiness sweep (all channels × 2 interests through
`Registration::poll_ready`, ~10⁶ calls/s) with per-channel wakers feeding a
fired-queue — Java NIO `selectedKeys()` O(ready) dispatch on pure tokio, no
socket/TLS/SASL/connect changes, no §8 deviation. Actor-Critic loop converged
CLEAN first pass; 1770 lib tests; 3 new mutation-checked regression tests.

| Arm | P28 | **P30** | Δ |
|---|---|---|---|
| A 200k/1KB latency-tuned | 76.2% | **74.2%** (p50=5 p99=12, RSS 28 MB) | **−2.0 pp** |
| B 200k 64KB-batch | 46.2% | **43.6%** (RSS 22 MB) | **−2.6 pp** |
| C big-batch 2KB/200p/4MB | 41.7% | **38.7%** (RSS 90 MB) | **−3.0 pp** |
| D 5k low-rate | 2.9% | **2.6%** | no idle regression |

`Registration::poll_ready` (5.8%) and `channel_interest` sweep (1.0%) are
GONE from the profile; ~3-4 pp of the removed dispatch reappeared as
selector/arming bookkeeping self-time (cycles redistribute — the recurring
lesson). Join stability: **5/5 consecutive fresh-group SASL_SSL KIP-848
joins, each 0.3-0.4 s to 24 assigned partitions.** Watch-item: `memset`
share grew 1.35→2.61% (not attributable to the P30 commits; monitor).

**Standing after Phase 30 — definitive 600 s-window pair (2026-06-12,
matched method, CRC off both):**

| 600 s window @200k latency-tuned | CPU | RSS end | p50/p99 |
|---|---|---|---|
| **librdkafka stock** | **52.4%** | 41.6 MB | 4/10 |
| **Rust (P30)** | **69.4%** | **27.9 MB** | 5/12 |

CRC-off gap = **1.32×**; with the measured CRC deltas (Rust +2.2 pp,
librdkafka +14.7 pp) the equal-work CRC-on estimate is ~71.6 vs ~67 ≈
**1.07×** (150 s-window measured CRC-on pair: 74.2 vs 66.7 = 1.11×).
Shorter Rust windows read ~2-5 pp high (KIP-848 join/settle ramp);
librdkafka shows no window sensitivity (52.0 ≈ 52.4). **RSS at matched
windows: Rust 27.9 MB < librdkafka 41.6 MB**, and librdkafka's RSS
trajectory ratchets at transient-hiccup intervals exactly like Rust's
(18.9→28→36→41.6 MB, flat between steps) — confirming the high-water
ratchet is workload/allocator physics, not a client property. Rust RSS
verified convergent over 10 min (steps by t=180, then flat ±1 MB; later
hiccups add no steps; see soak trajectory). CRC delta on the P30 binary:
2.2 pp (P27: 1.7 pp — consistent band). The journey at default
latency-tuned cloud SASL_SSL: ~130% (pre-P21) → 103 → 94 → 79 → 75 → 83†
→ 76.4 → 76.2 → **74.2%**. († cluster change + mimalloc removal re-based
the series; see sections above.)

---

### 30-min default-config @320 MB/s, 200 partitions (2026-06-12, cluster lkc-devcw75ngxj) — REVERSAL

First long-run default-config comparison at scale (the prior 1-hr run was
4 MB-batched). `bench-200p`, 2 KB records, single unbounded producer
(~160-168k msg/s ≈ 320 MB/s), default fetch (`fetch.min.bytes=1`,
`fetch.max.wait.ms=500`, batch 500), **CRC off on both** (librdkafka stock;
Rust via `check.crcs=false`), fresh warmup, 15 s `/proc` time-series,
113 samples each:

| 30 min | CPU avg | range | RSS | p50/p99/p99.9 |
|---|---|---|---|---|
| **Rust (P30)** | **103.0%** | 99.3–106.6 | **34 MB** | 5/22/37 |
| **librdkafka-C** | **128.2%** | 118.8–136.1 | 46 MB | 5/22/35 |

**Rust is ~20% more CPU-efficient at equal semantics in this regime**, with
identical latency (p50=5/p99=22 both; librdkafka max 178 vs 297 ms) and 26%
lower RSS.

Same-day/same-cluster 30-min control at **24 partitions** (1 KB, bounded
200k = 200 MB/s, CRC off both): Rust **75.2%** [73.1–77.3] RSS 39 MB vs
librdkafka **54.6%** [53.9–55.1] RSS 33 MB — ratio 1.38×, replicating the
previous cluster's 600 s pair; latency literally identical (p50=4 p99=9
p99.9=25-26 max=235 on BOTH). **Partition count confirmed as the decisive
regime variable** (all else equal-semantics, same day, same cluster,
30-min runs): librdkafka 1.38× ahead at 24p; Rust 1.24× ahead at 200p
(1.30× vs canonical per-message `poll()`).

Consume-API check (same regime, 600 s window): librdkafka **per-message
mode** (batch=1 ≡ `rd_kafka_consumer_poll()`, the canonical usage and what
every `poll()`-based binding does) = **133.6%** — per-message queue-pops
cost +5.4 pp over batch mode at ~164k msg/s. So the batch-mode 128.2% above
is librdkafka's BEST case (the methodology's deliberate choice), and against
canonical `poll()` usage the gap is Rust 103.0 vs 133.6 = **~23% in Rust's
favor**. Mechanism: at 200 partitions × tiny default fetches, librdkafka's
per-broker threads pay per-partition fetch/queue bookkeeping ×200; the
single-selector loop amortizes it. Combined with the earlier findings, the
regime map is now: **librdkafka leads only at low partition counts
latency-tuned (24p: 52 vs 69); parity at big-batch; Rust leads at
high-partition default-config scale.** (Note: cross-cluster vs the
lkc-devcnvw72vz numbers above; both arms same-cluster/same-day here.)

---

All four cells measured (same cluster/method/day). CRC32C costs librdkafka
**+14.7 pp** (software CRC on Graviton) but costs this port only **+1.7 pp**
(`crc32fast` uses the ARMv8 hardware CRC instructions — ~8.6× cheaper for
the same validation). **At Java-equivalent semantics (`check.crcs=true`,
the default workload of every Java-client migrator), the gap is Rust 76.2%
vs librdkafka 66.7% = 1.14×** — not 1.47×. At CRC-off for both it is 74.5%
vs 52.0% = 1.43×. The earlier 1.47×/1.60× headlines compared unequal work
(librdkafka skipping CRC); quote the 2×2, not one cell. The remaining
~9.5 pp at equal work ≈ tokio readiness machinery (~5-8 pp, the mio lever)
+ rustls's plaintext-chunk floor (~3-4 pp, upstream).

---

## Table 1 — Local 3-way (Rust vs Java vs librdkafka), the only run with all three

Local single host, **PLAINTEXT** localhost, 12 partitions, 300k msg/s, latency-tuned
(`fetch.min.bytes=1`, `max.poll.records=500`), **single 60 s run**.

| Client | Throughput | avg | p50 | p90 | p95 | p99 | p99.9 | max | CPU (1 core) | RSS |
|---|---|---|---|---|---|---|---|---|---|---|
| **Rust** | 300,676 | 3.4 | 1 | 6 | 10 | 63 | 196 | 209 | ~83% | ~30–59 MB |
| **Java** | 300,673 | **2.0** | 1 | 4 | 8 | **28** | 96 | **133** | **~76%** | heap 53–275 MB † |
| **librdkafka** (Python) | 257,572 ‡ | 3.7 | 1 | 4 | 11 | 82 | 244 | 275 | ~84% | ~46–52 MB |

latency in ms. † Java "RSS" = JVM **heap-used** (MemoryMXBean), not process RSS — **not comparable**. ‡ Python binding trailed ~15% (GIL/interpreter tax); native C does not (see Table 2).

---

## Table 2 — Local 3-way, **10-minute** run (most stable; all three clients)

Local PLAINTEXT, 12 partitions, 300k, latency-tuned, **10 min / 120 intervals each**
(noise averaged out). librdkafka-C uses batch consume (`rd_kafka_consume_batch_queue`)
for a fair comparison vs the stock 1-msg-per-poll API. All three on KIP-848.

| Client | Throughput | avg | p50 | p90 | p99 | max | CPU (1 core) | mem |
|---|---|---|---|---|---|---|---|---|
| **Rust** | 282k | 2.2 | 1 | 3 | **22** | 363 | 84.7% (65–94) | 41 MB RSS |
| **Java** | **299.5k** | 2.5 | 1 | 3 | 46 | **325** | 71.5% | heap 70–350 MB † |
| **librdkafka-C** | 280k | 3.4 | 1 | 4 | 57 | 661 | **49.7%** (32–54) | **31 MB RSS** |

latency in ms. † Java mem = JVM heap (GC sawtooth), **not** process RSS — not comparable.

**Takeaways:** throughput tied (all ~280–300k). **Latency: all p50 = 1 ms**, and Rust
& Java have the tighter tail (p99 22 / 46) vs C (57); Java the lowest max (325).
**CPU is the differentiator and the ranking is clear: librdkafka-C (49.7%) < Java
(71.5%) < Rust (84.7%)** — Rust ~1.7× C and ~1.2× Java; Java ~1.4× C. RSS: Rust ≈ C
(~31–41 MB).

---

## Table 3 — Confluent Cloud (8-CKU Dedicated), Rust vs librdkafka-C

In-region EC2 (us-east-1) → cluster, **SASL_SSL** (TLS + SASL/PLAIN), 24 partitions,
KIP-848, below saturation, single ~30–45 s runs. (Java not measured on cloud.)

| Rate | Client | Throughput | p50 | p99 | CPU (1 core) | RSS |
|---|---|---|---|---|---|---|
| **200k** | Rust | 201k | 5 | 23–42 | **~130%** | 28 MB |
| 200k | librdkafka-C | 200k | 5 | **21** | **~40%** | 40 MB |
| **300k** | Rust | 302k | 6 | 40–134 | **~140%** | 68 MB |
| 300k | librdkafka-C | 300k | 6 | **36** | **~60%** | 60 MB |

latency in ms. Throughput tied; p50 identical; librdkafka-C has the tighter tail and
**~2–3× lower CPU** over TLS. (Confluent Cloud requires SASL_SSL, so no plaintext
cloud arm.)

**Cloud ceiling note:** a single Rust consumer sustained **400k msg/s (391 MB/s)** on
this cluster (the 8-CKU cluster itself does ~400 MB/s ingress); 400k was the
*consumer's* edge (latency grew = saturation). Below it, latency is flat.

---

## Table 2b — Phase 21 confirmation: 20-min run, 200k plaintext (Rust vs librdkafka-C)

Local PLAINTEXT, 12 partitions, **200k** msg/s, latency-tuned, **20 min / 39 intervals
each** (most stable run we have for these two). Rust = **Phase 21** (dedicated
single-thread bg runtime). This is the run that isolates the Phase 21 CPU win.

| Client | Throughput | avg | p50 | p90 | p95 | p99 | p99.9 | max | CPU (1 core) | RSS |
|---|---|---|---|---|---|---|---|---|---|---|
| **Rust (Phase 21)** | 200,025 | 3.26 | 1 | 2 | 6 | 72 | 301 | **400** | **43.6%** (40–48) | **28 MB** |
| **librdkafka-C** | 200,030 | 2.32 | 1 | 2 | 5 | **35** | 273 | 548 | 38.2% (36–41) | 34 MB |

latency in ms. **Phase 21 took Rust @200k from ~66.7% → 43.6% CPU** — now only
**~1.14× librdkafka-C** (38.2%), down from ~1.7×. Memory is now *lower* than C
(28 vs 34 MB); throughput tied; p50 identical. librdkafka-C keeps the tighter p99
(35 vs 72), but Rust has the lower max (400 vs 548). Phase 21 closed ~85% of the
plaintext CPU gap with **zero behavior divergence** (internal execution-strategy
change only).

---

## Table 4 — CPU efficiency summary (the headline differentiator)

| Environment | librdkafka-C | Java | Rust | Rust ÷ C |
|---|---|---|---|---|
| Local plaintext, 300k (10-min, pre-Phase-21) | **49.7%** | 71.5% | 84.7% | ~1.7× |
| **Local plaintext, 200k (20-min, Phase 21)** | **38.2%** | — | **43.6%** | **~1.14×** |
| Cloud SASL_SSL, 200k (pre-Phase-21) | ~40% | — | ~130% | ~3.3× |
| Cloud SASL_SSL, 300k (pre-Phase-21) | ~60% | — | ~140% | ~2.3× |

CPU = % of one core. **Phase 21 (dedicated single-thread bg runtime) is the
inflection point:** it removed the tokio multi-thread scheduler park/unpark churn
(~54% of CPU in the profile) that came from running the single high-frequency bg
task on the caller's work-stealing pool. On local plaintext that drops Rust from
~1.7× → **~1.14× librdkafka-C** — effectively CPU parity. Cloud rows are still
pre-Phase-21; re-measuring cloud SASL_SSL with Phase 21 is the next step (expect a
similar drop, leaving the rustls-vs-OpenSSL TLS cost as the residual). Java not
measured on cloud.

The TLS amplification (1.7× → ~2–3×) is the rustls vs OpenSSL stream-processing cost
(see "Why" below).

---

## Headline findings

1. **Throughput — parity.** Rust, Java, and native librdkafka-C all track the offered
   load (200k–400k). The Python librdkafka binding trails ~15% (binding overhead, not
   the C client).
2. **Latency — competitive, p50 = 1 ms everywhere.** Java had the best tail in the
   local 3-way (p99 28 ms); Rust beat native-C on the 10-min local run (p99 22 vs 57);
   on cloud SSL native-C had the tighter tail. All within the same band; tails are
   noisy on single runs.
3. **CPU — the real differentiator, now largely closed locally by Phase 21.** Before
   Phase 21, Rust used ~1.7× (local plaintext) to ~2–3× (cloud SASL_SSL) the CPU of
   native librdkafka-C. **Phase 21 (dedicated single-thread bg runtime) brought local
   plaintext to ~1.14× C (43.6% vs 38.2% @200k, 20-min) — effectively parity, with
   lower memory.** The remaining gap is the cloud SASL_SSL TLS cost (rustls vs OpenSSL),
   not yet re-measured with Phase 21.
4. **Memory.** Rust and librdkafka-C are the same class (~30–50 MB process RSS); Java is
   a GC heap sawtooth (not directly comparable).

## Why Rust's CPU is higher (investigated via `perf` + code review)

- **NOT** tokio/async overhead, and **NOT** the crypto (~5%). It is **distributed**
  across the per-poll / per-record path: fetch decode, the poll/collect machinery, and
  — on SSL — rustls TLS stream processing (deframer + record handling ≈ 40% of SSL CPU,
  a rustls-vs-OpenSSL structural difference).
- Genuine waste *was* found and fixed (a fetch payload copied up to 3×, per-request
  `ApiVersions` clones — they violated the zero-copy contract). Fixing them recovered
  **~3–4% CPU + ~43% RSS** (68→39 MB at 300k) with no throughput/latency change — real,
  but not enough to close the headline gap, which is structural/distributed, not one bug.

## Methodology & caveats (for credibility)

- **Single runs are noisy** except Table 2 (10-min). Treat tail percentiles (p99+) as
  same-order-of-magnitude, not precise rankings; differences < ~2× on the tail are noise.
- **Fair-comparison fixes applied:** librdkafka-C must use **batch consume**
  (`rd_kafka_consume_batch_queue`, return-available) — the stock `rd_kafka_consumer_poll`
  returns 1 msg/call (unfair). All arms forced to **KIP-848** (`group.protocol=consumer`)
  for same-protocol comparison.
- **CPU sampling:** local via `sysinfo` (per-process, % of one core); cloud via
  `/proc/<pid>/stat`. Java "RSS" is JVM heap-used (not process RSS).
- **Java arm** = the reference `E2ELatencyBenchmark`, measured **local only** (no cloud).
- **Confluent Cloud** requires SASL_SSL → no plaintext cloud arm; the cloud CPU
  therefore includes TLS, which is why the Rust gap is larger there.
- The earlier "~150 MB/s ceiling" and "21 s latency" observations were rig artifacts
  (single producer / 6 partitions; India↔us-east-1 WAN) — not cluster or client limits.
