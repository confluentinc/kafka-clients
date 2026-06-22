# Cloud consumer benchmarks — 2026-06-15

Rust vs librdkafka vs Java (kafka-clients) consumer e2e-latency / CPU / RSS, against
real Confluent Cloud over SASL_SSL, in-region (us-east-1).

## Start here for graphing
- **`summary.csv`** — one row per run, all aggregate metrics. Use this for bar charts
  (client × metric). `throughput_msg_s` is the accurate throughput; derive MiB/s as
  `throughput_msg_s * msg_bytes / 1048576` (do **not** trust harness-reported MiB/s — see caveats).
- **Time-series** (per-interval, for line charts over time):
  - Rust + librdkafka: `*/metrics.jsonl` (one JSON object per interval; `type":"interval"` lines).
  - Java: `*_java_intervals.csv` (parsed from the run logs; Java has no JSONL).
  - CPU/RSS over time for Rust & Java: `*_cpu.csv` (external `/proc` sampler).

## Environment
- Cluster `lkc-devc22rvqgy` / `pkc-devcozvdmy` (12 CKU Dedicated, **single-zone `use1-az5`**), us-east-1.
- Topics: `cmp-bench-200p` (200 partitions), `cmp-librdkafka-bench` (12 partitions), RF3, 5-min retention.
- Consumer host: EC2 m8g.4xlarge (Graviton, AL2023). **same-az** runs = box in `use1-az5`
  (co-located w/ cluster). **cross-az** runs = box in `use1-az1` (cluster in `use1-az5`).
- Rust = post-Phase-30 release build. librdkafka = source build (repo harness `librdkafka_e2e.c`,
  and the GitHub-port `benchmark_e2e_latency.c` = run "B"). Java = `JavaE2E.java` on kafka-clients 4.0.0.

## Common config (all runs unless noted)
`group.protocol=consumer` (KIP-848), `fetch.min.bytes=4 MiB`, `max.partition.fetch.bytes=4 MiB`,
`check.crcs=false`, `auto.offset.reset=latest`. Producer = `kafka-producer-perf-test` (acks=1).
Per-run: `batch_or_maxpoll` column (librdkafka = consume-batch size; Rust/Java = `max.poll.records`).

## Methodology (latency)
All harnesses use an **exact 1 ms histogram + per-batch clock** (`now − record.timestamp()` once
per poll) and message-count warmup — directly comparable — **except run "B"**
(`librdkafka-ghport-B`), which uses **reservoir percentiles + per-record clock + per-record
byte-sum**. B's latency tail and CPU are NOT methodology-matched; treat it separately.

## Caveats (important for honest graphs)
- **MiB/s artifact:** the Rust & librdkafka harnesses compute bytes from a `message_size` arg that
  defaulted to **1 KB**. For the 2 KB runs (`longrun-200p-2kb`) their reported MiB/s is ~half the
  true value. `msg/s` is always correct. `summary.csv` therefore records `msg/s` only.
- **CPU source:** Rust & Java CPU = external `/proc` sampler (the in-harness sampler reads 0% on
  aarch64). librdkafka CPU = in-harness `getrusage`. Run B's CPU is inflated by its own per-record
  byte-sum + background writer + `statistics.interval.ms` callback (~1.4× overhead measuring the same client).
- **RSS:** current RSS (`/proc` VmRSS for Rust/Java; `/proc/self/statm` for the updated librdkafka
  harness). The cross-az Run A predates that change and reports **peak `ru_maxrss`**.
  Java RSS grows into the JVM heap (no `-Xmx`); the 200p longrun climbed 1.1→7.5 GB (avg ~4.8 GB).
- **`lat_stddev_ms` blank** for runs that predate the stddev addition (cross-az Run A, Rust 200k/peak, Java peak).
- 60 s runs (`quick60`): p50/p90/p99 solid; p99.9/max are noisy (few tail samples).
- Cross-AZ vs same-AZ latency is ~unchanged (4 MiB fetch dominates; cross-AZ ≈ 1 ms); same-AZ's real
  win is cross-AZ $ cost. e.g. librdkafka 142.31 (cross-az) ≈ 142.33 (same-az) at 12p/1KB/200k.

## Headline (same-az 200p/2KB/~300 MB/s, 30 min)
Throughput tied (~150k). Latency: librdkafka p50 254 < Java 281 ≈ Rust 286 (Rust≈Java, both ~30 ms
over native librdkafka). CPU: Rust 33.8% ≈ librdkafka 35.8% < Java 58.5%. RSS: Rust 115 MB ≪
librdkafka 327 MB ≪ Java ~4.8 GB. Rust's one weak spot = deep tail (p99.9 763 / max 1359).

## File layout
```
same-az/
  lr_rust.log  consumer-perf/results/lr-rust-*/metrics.jsonl   lr_rust_cpu.csv      (Rust 200p/2KB 30m)
  lr_repo.log  lr_repo/lr-repo-*/metrics.jsonl                                      (librdkafka 200p/2KB 30m)
  lr_java.log  lr_java_intervals.csv                            lr_java_cpu.csv     (Java 200p/2KB 30m)
  q3_*.log  q3_repo/*/metrics.jsonl  consumer-perf/results/q3-rust-*/  q3_*_cpu.csv  q3_java_intervals.csv  (60s quick 3-way)
cross-az/
  runA.log  results_repo/cmp-repo-*/metrics.jsonl                                   (librdkafka A, 200k/12p)
  runB.log  results_gh/*/metrics.jsonl (+results.json,summary.txt,librdkafka_stats) (librdkafka B github-port)
  runRust.log + consumer-perf/results/  rust_cpu.csv                                (Rust 200k/12p)
  runRustPeak.log  rustpeak_cpu.csv                                                 (Rust peak 1-producer)
  runJava.log  runJava_intervals.csv  java_cpu.csv                                  (Java 200p/2KB peak)
```

---

## Default-config run group (`default-200p-2kb-300mbs`, added 2026-06-16)

Same workload (200p / 2 KB / ~300 MB/s, 30 min, same-az `use1-az5`) but **default/out-of-box configs**:
`fetch.min.bytes=1`, `max.poll.records=500` (Rust/Java), and **librdkafka using single-message
`rd_kafka_consumer_poll()`** (the common real-world pattern). Data in `same-az-default/`.

**Result — latency converges, CPU differentiates:**

| | avg/p50/p99/p99.9/max (ms) | CPU | RSS |
|---|---|---|---|
| librdkafka (single `poll()`) | 4.94 / 4 / 20 / 34 / 247 | ~122% | 77 MB |
| Rust (batch 500) | 5.31 / 4 / 20 / 36 / 265 | **109.6%** | **34.5 MB** |
| Java (batch 500) | 5.32 / 4 / 21 / 35 / 235 | 114.5% | 881 MB |

- **Latency: 3-way tie (~5 ms, p99 ~20 ms).** With `fetch.min.bytes=1` every fetch returns
  immediately, so the ~30 ms native-client edge seen at 4 MiB disappears.
- **CPU: Rust lowest** (110%), < Java (114.5%) < librdkafka-single-poll (122%). All are CPU-heavy
  (~110–122%) because small fetches → frequent polls. librdkafka's higher figure is the
  **single-message-`poll()` penalty** (150k calls/s), not a batched-client number.
- **RSS: Rust ≪ librdkafka ≪ Java** (34 / 77 / 881 MB).

**The latency-vs-CPU knob (same workload, two configs):**

| config | p50 / p99 latency | CPU |
|---|---|---|
| 4 MiB fetch, batch 2000/2500 (`longrun-200p-2kb`) | ~255–286 / 522–580 ms | ~34–58% |
| default (fetch.min=1, batch 500) | 4 / ~20 ms | ~110–122% |

**Caveats specific to this group:**
- librdkafka uses **single-message `poll()`** (`--single-poll`); Rust/Java batch up to 500.
- **CRC follows each client's default** here (not forced off): Apache Rust/Java = **on**, librdkafka = **off**.
- MiB/s artifact still applies (Rust/librdkafka report 146 MiB/s at the 1 KB assumption; real ~293; Java's 293 is correct). Use `throughput_msg_s`.
- Java RSS (881 MB) is JVM heap, far below the 4 MiB run's ~7 GB (smaller default 1 MB fetch buffers → less allocation pressure).

`same-az-default/` files: `ld_rust.log` + `consumer-perf/results/ld-rust-*/metrics.jsonl` + `ld_rust_cpu.csv` (Rust);
`ld_repo.log` + `ld_repo/ld-repo-*/metrics.jsonl` (librdkafka single-poll); `ld_java.log` + `ld_java_intervals.csv` + `ld_java_cpu.csv` (Java).

---

## 36-partition default-config run (`default-36p-2kb-300mbs`, added 2026-06-17)

Same default config as Workload 2 (`fetch.min.bytes=1`, `max.poll.records=500`, librdkafka single-`poll()`),
but on a **36-partition topic** (~1 partition per broker) with a **4-min warmup (36M msgs) + 30-min measure**.
Run on the **TLS-fix binary** (commit `d5b0e50`, "classify TLS-handshake reset as retriable"). Data in `same-az-36p-default/`.

| Client | Throughput | avg | p50 | p99 | p99.9 | max | stddev | CPU | RSS |
|---|---|---|---|---|---|---|---|---|---|
| **librdkafka** (single `poll()`) | ~150k msg/s | 4.17 | 4 | 16 | 30 | 234 | 2.92 | ~85% | 54 MB |
| **Rust** | ~150k msg/s | 4.80 | 4 | 18 | 35 | 255 | 3.68 | **104%** | **38 MB** |
| **Java** | ~150k msg/s | 4.48 | 4 | 17 | 31 | 228 | 2.94 | 108% | 674 MB |

**Takeaways:**
- **All three hold 150k msg/s at ~4–5 ms / p99 ~17 ms** — the clean low-latency regime, same as the 200p default. 36 partitions (~1/broker) absorb ~300 MB/s with ~2 ms produce latency.
- **CPU:** librdkafka-single-poll lowest (~85%), then Rust (104%), Java (108%) — all ~1 core (small-fetch/frequent-poll). With fewer partitions than the 200p run, Rust's CPU eased 110%→104%.
- **RSS:** Rust leanest (38 MB) < librdkafka (54 MB) ≪ Java (674 MB JVM heap).
- The 4-min warmup cleanly excluded Rust's startup ramp — its per-interval p50/p99 are flat for the full 30 min (see `workload3_36p_default_latency_over_time.png`).

**Note on partition count:** a prior attempt at **10 partitions** / ~300 MB/s was *producer-bound* — 10 partitions can't absorb 300 MB/s (~210 ms produce buffering), so it was aborted. **36 partitions** is the right count for this rate (no producer bottleneck), which is why this run cleanly isolates consumer behavior.

`same-az-36p-default/` files: `ld36_rust.log` + `rust-results/ld-rust-*/metrics.jsonl` + `ld36_rust_cpu.csv` (Rust);
`ld36_repo.log` + `ld36_repo/ld-repo-*/metrics.jsonl` (librdkafka single-poll); `ld36_java.log` + `ld36_java_intervals.csv` + `ld36_java_cpu.csv` (Java).
Charts: `workload3_36p_default_latency_over_time.png`. Plot scripts: `plot_latency.py`, `plot_36p.py`.
