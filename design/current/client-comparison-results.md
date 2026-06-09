# Consumer client comparison — Rust vs Java vs librdkafka

Collated results, June 2026. Single consumer, KIP-848 (`group.protocol=consumer`),
1 KiB records, `auto.offset.reset=latest` (live e2e latency = `consume_time −
record.timestamp`). CPU is **% of one core** (>100% = multiple cores; consumers are
multi-threaded). All clients keep up with the offered rate unless noted.

> **Read CPU within a table, not across tables** — each environment differs
> (partitions, plaintext vs TLS, 1 vs 16 cores, sampler). The cross-environment CPU
> *ratios* are the durable signal.

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

## Table 4 — CPU efficiency summary (the headline differentiator)

| Environment | librdkafka-C | Java | Rust | Rust ÷ C |
|---|---|---|---|---|
| Local plaintext, 300k (10-min) | **49.7%** | 71.5% | 84.7% | **~1.7×** |
| Cloud SASL_SSL, 200k | ~40% | — | ~130% | ~3.3× |
| Cloud SASL_SSL, 300k | ~60% | — | ~140% | ~2.3× |

CPU = % of one core. Ordering (local, all 3): **librdkafka-C < Java < Rust**
(Rust ~1.2× Java, ~1.7× C; Java ~1.4× C). Java not measured on cloud. The
local→cloud jump for Rust (1.7×→~2–3×) is the rustls-vs-OpenSSL TLS cost on SSL.

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
3. **CPU — the real differentiator.** Native librdkafka-C is the most CPU-frugal:
   Rust uses **~1.7× (local plaintext)** to **~2–3× (cloud SASL_SSL)**. Java is roughly
   in Rust's class locally. This is the gap to close for Rust as a librdkafka replacement.
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
