# 30-min e2e consumer comparison @ 300 MB/s — Rust vs librdkafka vs Java

**Date:** 2026-06-29/30 (UTC)
**Rig:** AWS us-east-2 (Ohio) ARM Graviton, Amazon Linux 2023 aarch64, 16 vCPU / 61 GiB.
**Cluster:** Confluent Cloud DEDICATED **12 CKU, single-AZ (`use2-az1`)**, `lkc-devco327m2o` /
`pkc-devcg1y1mn`, in-region (same AZ as the client box).
**Topic:** `cmp-bench-36p` (36 partitions, RF=3), retention capped 4 GB/part during the run.
**Workload:** 150,000 msg/s × 2 KB = **~300 MB/s** (4 parallel `kafka-producer-perf-test.sh`
producers, `acks=1`), KIP-848 `group.protocol=consumer`, SASL_SSL.
**Per client:** consumer started first; **4-min warmup (36M msgs) excluded**; **30-min measured**.
**Config (matched, latency-tuned):** `fetch.min.bytes=1`, `max.partition.fetch.bytes=1 MiB`,
`max.poll.records=500`; librdkafka adds `--single-poll` + `check.crcs=false` (CRC-aligned).
**Rust binary:** consumer-impl **+ PR #116 CPU improvements** (zero-copy `bytes::Bytes` receive
path, varint fast path, generator update) **with the metrics impl intact**.

## Results (270,000,000 messages measured per client)

| metric            | Rust (PR#116+metrics) | librdkafka (C) | Java (kafka-clients) |
|-------------------|----------------------:|---------------:|---------------------:|
| throughput        | 150,000 msg/s         | 150,000 msg/s  | 149,997 msg/s        |
| e2e avg (ms)      | 3.87                  | 3.52           | 3.71                 |
| e2e p50 (ms)      | 4                     | 3              | 3                    |
| e2e p99 (ms)      | 10                    | 10             | 9                    |
| e2e p99.9 (ms)    | 27                    | 26             | 26                   |
| e2e max (ms)      | 252                   | 350            | 245                  |
| **CPU mean**      | **112%** (110–121)    | **94%** (92–96)| **113%** (111–117)   |
| **RSS mean**      | **37 MB**             | 61 MB          | 724 MB (JVM)         |

## Takeaways

- **Latency: three-way tie.** ~3.5–3.9 ms avg, p50 3–4, p99 9–10, p99.9 26–27 across all three.
  The Rust consumer is fully latency-competitive at 300 MB/s.
- **Throughput: tied** — all sustained 150k msg/s for the full 30 min (270M msgs each).
- **CPU: Rust (112%) ≈ Java (113%); librdkafka most efficient at 94%** (~1.2× gap). Tight,
  stable bands (no regime bistability — the 4-min warmup locked all clients into steady state).
- **Memory: Rust wins decisively — 37 MB** vs librdkafka 61 MB vs Java 724 MB (JVM heap).

## PR #116 CPU improvement: no measurable gross-CPU change at this rate
Rust with the PR #116 zero-copy `bytes::Bytes` receive path measured 112% — same as the
pre-improvement baseline (~110%), within noise. A `perf` self-time profile explains why and
points at the real sinks; see `OPTIMIZATION-ANALYSIS.md` and `rust_perf_selftime.txt`.
Summary: TLS/socket receive ~18–20% (inherent), allocator churn ~10%, HashMap lookups ~6.5%,
metrics ~2.4%; the PR's fetch-decode target is only ~3–4% of CPU, so zero-copy there can't
move the gross number. Design-preserving next levers: FxHashMap on per-poll maps, per-poll
scratch reuse, fewer Selector re-lookups (details in `OPTIMIZATION-ANALYSIS.md`).

## Files
- `rust.log` / `rdk.log` / `java.log` — per-arm interval trends + summary
- `rust_cpu.csv` / `rdk_cpu.csv` / `java_cpu.csv` — /proc CPU% + RSS sampled every 10 s
- `rust_perf_selftime.txt` / `rust_perf_callgraph.txt` — `perf -F299` profile of the Rust consumer
- `OPTIMIZATION-ANALYSIS.md` — design-preserving CPU optimization opportunities
- `run30.log` — driver timeline
