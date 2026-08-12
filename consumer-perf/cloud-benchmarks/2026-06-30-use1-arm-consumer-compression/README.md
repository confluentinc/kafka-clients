# Consumer-with-compression (snappy/gzip + none anchor) — us-east-1, finishing the sweep

Companion to `2026-06-30-use2-arm-consumer-compression/` (none/zstd/lz4). Run here after the
us-east-2 cluster's key expired; covers **none, snappy, gzip × rust/librdkafka/java**.
Box: us-east-1 m8g.4xlarge (ARM). Cluster: 12-CKU `pkc-devcozvdmy`. Topic `cmp-bench-200p`.
Feed: 4× kafka-producer-perf-test @150k, 2KB, compression.type=<codec>, batch.size=1MB,
linger.ms=5. Consumer: --no-produce, 2-min warmup + 10-min, fetch.min=1/maxpoll=500.
CRC rust on+metrics / librdkafka off / java on. All sustained 150k (293 MiB/s); CPU = /proc.

## Results (CPU% one core; ΔCPU vs none; RSS MB; eff = msg/s per 1% CPU)

| codec  | rust CPU | rust e2e avg/p99 | rust RSS | lib CPU | java CPU |
|--------|---------:|-----------------:|---------:|--------:|---------:|
| none   | 41       | 16.8 / 41 ms     | 57       | 43      | 71       |
| snappy | 47 (+6)  | 17.6 / 42        | 54       | 45 (+2) | 80 (+9)  |
| gzip   | 84 (+43) | 15.2 / 31        | 29       | **207 (+164)** | 109 (+38) |

## Key finding — gzip DECOMPRESS is a Rust strength (opposite of gzip COMPRESS)
- Producer gzip: Rust worst (single-thread + miniz_oxide deflate slow → 31k collapse).
- Consumer gzip: **Rust most CPU-efficient** — 84% (eff 1786) vs librdkafka 207% (eff 725)
  vs java 109% (eff 1376). miniz_oxide *inflate* is fast even though its *deflate* is slow;
  librdkafka gzip-decompress is unexpectedly ~2 cores.
- No consumer collapse on any codec (all kept 150k). Decompression ≪ compression cost.
- Rust most CPU-efficient consumer for EVERY codec; leanest RSS (29-57 vs lib 99-105 vs
  java 645-767 JVM). Latency tied (~13-19ms). See use2 dir for none/zstd/lz4.
