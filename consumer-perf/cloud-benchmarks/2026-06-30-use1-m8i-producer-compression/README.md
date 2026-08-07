# Producer compression matrix — rust-native vs librdkafka @ 300 MB/s, 200p, 12-CKU

**Date:** 2026-06-30. **Box:** AWS us-east-1 **m8i.4xlarge** (Intel, 16 vCPU, ~7.5 Gbps).
**Cluster:** Confluent Cloud DEDICATED **12-CKU** (`pkc-devcozvdmy`, 36 brokers), in-region.
**Topic:** `cmp-bench-200p` (200 partitions, RF=3). **SASL_SSL.**
**Workload (your spec):** 2 KB messages, **max-rate** (`LIMIT_RPS=0`), `acks=all`,
`enable.idempotence=false`, `batch.size=1MB`, `max.request.size=8MB`, `linger.ms=5`,
`buffer.memory=32MB`, `max.in.flight = 1000 (librdkafka) / 5 (rust-native)`.
**Per run:** 2-min warmup + **10-min measured**. Codec set: none, zstd, lz4, snappy, gzip.

## Results (max-rate; CPU% of one core; RSS MB; eff = msg/s per 1% CPU; lat = max-rate, backpressure-dominated)

| codec | backend | msg/s | MiB/s | CPU% | RSS MB | p99 lat | avg lat | **eff (msg/s/1%CPU)** |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| **none** | rust-native | 288,612 | 564 | 115 | 147 | 515 | 213 | 2,508 |
| | librdkafka | 287,324 | 561 | 172 | 359 | 254 | 129 | 1,667 |
| | java | 287,222 | 561 | 67 | 1,793 | — | — | **4,280** |
| **lz4** | rust-native | 432,378 | 844 | 189 | 107 | 137 | 22 | 2,288 |
| | librdkafka | 505,983 | 988 | 306 | 2,466 | 91 | 44 | 1,652 |
| | java | 546,169 | 1,067 | 120 | 2,846 | — | — | **4,555** |
| **snappy** | rust-native | 444,261 | 868 | 176 | 160 | 298 | 143 | 2,518 |
| | librdkafka | 489,244 | 956 | 307 | 2,286 | 98 | 49 | 1,595 |
| | java | 424,167 | 828 | 94 | 2,494 | — | — | **4,493** |
| **zstd** | rust-native | 269,718 | 527 | 151 | 161 | 33 | 14 | 1,790 |
| | librdkafka | 477,330 | 932 | 523 | 1,112 | 108 | 56 | 913 |
| | java | 250,443 | 489 | 134 | 718 | — | — | **1,875** |
| **gzip** | rust-native | 30,889 | 60 | 106 | 63 | 22 | 8 | 292 |
| | librdkafka | 248,827 | 486 | 776 | 419 | 286 | 130 | 321 |
| | java | 45,340 | 89 | 112 | 380 | — | — | **406** |

**CPU efficiency (msg/s per 1% CPU) — full 3-way:** **Java is the *most* CPU-efficient producer**
on none/lz4/snappy (eff 4280–4555), **Rust 2nd** (2288–2518), **librdkafka least** (1595–1667).
On zstd, Java ≈ Rust (1875 ≈ 1790) ≫ librdkafka (913). On gzip all are poor (Java 406 > lib 321
> Rust 292). So Rust beats librdkafka per-CPU on every codec except gzip, but **Java beats both**.
**RSS — Rust dramatically leanest** (60–161 MB) vs librdkafka 359 MB–2.5 GB vs **Java 0.7–2.8 GB (JVM)**.
**Latency** = max-rate (backpressure), not a clean codec signal — Java max-rate latency omitted.

## Mechanism — single-thread compression caps the Rust producer
Rust (like Java) compresses on its **single Sender task**; librdkafka parallelizes
compression across per-broker threads. At max-rate, Rust throughput =
**min(cluster byte-ceiling, single-thread compression rate of the codec)**:

- **none:** both ~288k = the **12-CKU cluster ingress ceiling** (~576 MB/s). Not client-limited
  — and Rust hits it at **115% vs librdkafka 172%** (≈⅔ the CPU). Efficiency win.
- **lz4 / snappy (fast codecs):** sender compresses faster than it sends, so it's not the
  bottleneck; compression shrinks wire bytes → more msgs fit under the cluster ceiling →
  throughput **rises** (432k / 444k, up from 288k). Rust competitive with librdkafka.
- **zstd (moderate):** single-thread zstd (~270k msg/s on one core) becomes the bottleneck,
  *below* the uncompressed rate → throughput **drops to 270k** despite higher CPU (151%).
  librdkafka parallelizes zstd across ~5 cores (523%) → 477k.
- **gzip (expensive):** single-thread gzip collapses to **31k @ 106% (1 core)**; librdkafka
  throws ~8 cores (776%) at it → 249k. **~8× gap.** Compounded by the Rust client using
  `flate2`'s default pure-Rust `miniz_oxide` deflate (slower than Java/librdkafka's native
  zlib) — fixable via flate2's `zlib-ng` backend; the single-thread half is architectural.

## Takeaways
- **Efficiency:** at the cluster ceiling (none) and for cheap codecs (lz4/snappy), Rust is
  throughput-competitive at **lower CPU** than librdkafka.
- **Single-producer compressed ceiling:** for moderate/heavy codecs (zstd, gzip) Rust's
  max throughput is **single-thread-compression-bound** and below librdkafka's multi-threaded
  ceiling. Matters only for max-throughput-compressed from ONE producer instance; mitigations:
  scale out producer instances (each adds a sender thread) or move compression off the sender
  thread (a divergence from the Java-faithful single-Sender design). **gzip is the worst case.**
- Latency columns omitted (max-rate → backpressure-dominated, noisy; not the signal here).

## Files
- `<backend>__<codec>.log` — per-arm summaries (rate, MiB/s, CPU, RSS)
- `matrix.log` — run timeline
