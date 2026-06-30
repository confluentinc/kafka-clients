# Producer compression matrix — rust-native vs librdkafka @ 300 MB/s, 200p, 12-CKU

**Date:** 2026-06-30. **Box:** AWS us-east-1 **m8i.4xlarge** (Intel, 16 vCPU, ~7.5 Gbps).
**Cluster:** Confluent Cloud DEDICATED **12-CKU** (`pkc-devcozvdmy`, 36 brokers), in-region.
**Topic:** `cmp-bench-200p` (200 partitions, RF=3). **SASL_SSL.**
**Workload (your spec):** 2 KB messages, **max-rate** (`LIMIT_RPS=0`), `acks=all`,
`enable.idempotence=false`, `batch.size=1MB`, `max.request.size=8MB`, `linger.ms=5`,
`buffer.memory=32MB`, `max.in.flight = 1000 (librdkafka) / 5 (rust-native)`.
**Per run:** 2-min warmup + **10-min measured**. Codec set: none, zstd, lz4, snappy, gzip.

## Results (msg/s, MiB/s = uncompressed payload rate, CPU% of one core)

| codec   | rust-native rate | rust MiB/s | rust CPU | librdkafka rate | lib MiB/s | lib CPU |
|---------|-----------------:|-----------:|---------:|----------------:|----------:|--------:|
| none    | 288,612          | 564        | **115%** | 287,324         | 561       | 172%    |
| lz4     | 432,378          | 844        | 189%     | 505,983         | 988       | 306%    |
| snappy  | 444,261          | 868        | 176%     | 489,244         | 956       | 307%    |
| zstd    | 269,718          | 527        | 151%     | 477,330         | 932       | 523%    |
| **gzip**| **30,889**       | **60**     | **106%** | 248,827         | 486       | 776%    |

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
