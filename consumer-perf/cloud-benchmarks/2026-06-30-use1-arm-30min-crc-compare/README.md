# 30-min controlled CPU comparison — does PR #116 narrow the Rust↔librdkafka gap?

**Goal:** isolate whether the PR #116 consumer CPU improvements (zero-copy `bytes::Bytes`
receive path) actually reduce the Rust↔librdkafka CPU gap — by running **librdkafka,
Rust-baseline (pre-PR#116), and Rust-improved (PR#116)** in one session on the **same
cluster**, so the gap is measured against an identical, simultaneously-measured librdkafka
reference (absolute CPU has ~±10% run-to-run/cluster variance, so only the within-session
gap is trustworthy).

**Date:** 2026-06-30 (UTC). **Box:** AWS us-east-1 ARM Graviton (m8g), AL2023 aarch64, 16 vCPU.
**Cluster:** Confluent Cloud `pkc-devcozvdmy` / `lkc-devc22rvqgy` (recovered from a
demoted-broker incident earlier; healthy ~1.8 ms produce-ack at 300 MB/s).
**Topic:** `cmp-bench-36p` (36p, RF=3). **Workload:** 150k msg/s × 2 KB = ~300 MB/s,
4 producers `acks=1`, KIP-848, SASL_SSL. **Per arm:** consumer-first, 4-min warmup (36M
msgs) excluded, 30-min measured. **Config:** `fetch.min.bytes=1`,
`max.partition.fetch.bytes=1 MiB`, `max.poll.records=500`; librdkafka `--single-poll`.

**CRC / metrics (intentional — each client in its realistic default):**
- **Rust: CRC on + metrics on.** `check.crcs` defaults to `true` in the Rust client
  (consumer_config.rs), and the consumer metrics framework is compiled in. This is the
  going-forward default config.
- **librdkafka: CRC off** (`check.crcs=false`) — librdkafka's own default; most deployments
  run it this way. Comparing each client as it actually ships.

## Results (270,000,000 messages measured per arm)

| arm                              | CPU    | RSS   | e2e avg | p50 | p99 | p99.9 | max  |
|----------------------------------|-------:|------:|--------:|----:|----:|------:|-----:|
| librdkafka (crc-off)             | **92.1%** | 61 MB | 3.81 ms | 3 | 15 | 30 | 278  |
| Rust-baseline (crc-on + metrics) | **111.3%**| 43 MB | 4.56 ms¹| 4 | 17 | 33 | 3543 |
| Rust-improved (PR#116)           | **111.9%**| 38 MB | 4.34 ms | 4 | 16 | 30 | 306  |

¹ baseline avg inflated by one transient interval (max 3543 ms); its p50/p99 (4/17) match improved.

- gap_baseline  = 111.3 − 92.1 = **+19.2 (1.21×)**
- gap_improved  = 111.9 − 92.1 = **+19.8 (1.22×)**

## Verdict
- **PR #116 does NOT narrow the Rust↔librdkafka CPU gap.** Baseline and improved are both
  ~111–112% against the same 92.1% librdkafka reference (gap ~1.21×, indistinguishable —
  improved is marginally *higher*, i.e. within noise). The zero-copy change targets a
  ~3–4% slice of CPU dominated by TLS (~18–20%, inherent) + allocator (~10%) + HashMap
  (~6.5%); see the us-east-2 `perf` profile + `OPTIMIZATION-ANALYSIS.md`.
- **Cross-cluster consistent:** us-east-1 here (111 vs 92 = 1.21×) ≈ us-east-2 30-min
  (112 vs 94 = 1.19×). The Rust consumer runs **~1.2× librdkafka CPU** at 300 MB/s in its
  default config (crc-on + metrics) vs librdkafka default (crc-off).
- **Latency tied** (p50 3–4, p99 15–17). **Rust RSS leaner** (38–43 vs 61 MB).
- The earlier "84% librdkafka last week vs 92% now" (both crc-off ARM 2.13.0, same cluster)
  is run-to-run/cluster-state variance, not a setup/version/producer-count difference — the
  same ±10% envelope librdkafka swung across all runs (84 / 92 / 94). This is exactly why the
  three arms are run in one session and the *gap* is read, not the absolute.

## Files
- `rdk.log` / `rust_baseline.log` / `rust_improved.log` — interval trends + summaries
- `*_cpu.csv` — /proc CPU% + RSS every 10 s
- `run.log` — driver timeline
