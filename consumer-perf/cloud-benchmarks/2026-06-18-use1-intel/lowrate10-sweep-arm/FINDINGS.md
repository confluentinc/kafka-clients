# 10-partition CPU-vs-throughput sweep (ARM, USE1 CCloud)

**Setup:** Graviton ARM EC2, in-region to USE1 CCloud cluster (SASL_SSL/PLAIN).
Topic `cmp-bench-10p` (10 partitions), 2 KB records, default consumer config
(`fetch.min.bytes=1`, `check.crcs` left at each client's default — Rust on,
librdkafka off). 5-min steady-state runs each, ~30s warmup, sequential (shared
topic), CPU sampled from `/proc/PID/stat` (utime+stime), latency e2e (produce→consume).

- Rust = `consumer-perf` (with-metrics build), self-produce (1 producer).
- librdkafka = `librdkafka_e2e` (default, no stats), external `kafka-producer-perf-test`.

The 5 MB/s point is from the earlier `lowrate10-arm/` run; 10/20/50 MB/s from this sweep.

## Results

| 10p, 2 KB | 5 MB/s (2500/s) | 10 MB/s (5000/s) | 20 MB/s (10000/s) | 50 MB/s (25000/s) |
|---|---|---|---|---|
| **Rust CPU**       | 3.0% | 5.6%  | 11.0% | 24.6% |
| **librdkafka CPU** | 2.0% | 3.5%  | 6.7%  | 15.6% |
| Rust p50 / p99 (ms)       | 3 / 11 | 3 / 11 | 3 / 11 | 3 / 13 |
| librdkafka p50 / p99 (ms) | 3 / 11 | 3 / 11 | 3 / 11 | 3 / 12 |
| Rust RSS (MB)       | 13 | 19 | 20 | 22 |
| librdkafka RSS (MB) | 19 | 20 | 22 | 25 |

## Findings

1. **CPU scales ~linearly with rate for both clients.** Rust ≈ 0.5% CPU per
   1 MB/s; librdkafka ≈ 0.3%. Rust sits ~1.5–1.6× librdkafka across the whole
   sweep — consistent with the single-Selector architectural cost already
   characterized in `consumer_lowrate_cpu_profile`. The ratio does NOT widen
   with rate; it is a roughly constant multiplier, not a divergence.

2. **Latency is effectively identical.** p50 = 3 ms everywhere; p99 pinned at
   11 ms up to 20 MB/s, ticking to 12–13 ms at 50 MB/s for both. No tail
   penalty for Rust at low partition counts — the "fat tail" investigated
   earlier was environmental, not a client property (see
   `consumer_default_config_tail_latency`).

3. **RSS small and comparable.** Rust 13–22 MB, librdkafka 19–25 MB. Rust is
   actually leaner here (1-producer harness; the earlier "fat memory" was a
   4-producer artifact).

4. **Absolute CPU is low.** Even at 50 MB/s on a 10p topic the Rust consumer is
   under a quarter of one core. The CPU gap matters only as a percentage at low
   load; in absolute terms both are cheap.

(librdkafka's reported MiB/s is half of Rust's for the same msg/s — a harness
payload-accounting difference; the msg/s rate and latency are the comparable
figures and match exactly.)
