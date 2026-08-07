# Metrics-on perf batch (2026-06-22/23) — Rust metrics vs librdkafka statistics

Goal: cost of metrics-on across clients. Rust = with-metrics binary (M1-M3 + uncommitted);
librdkafka = `statistics.interval.ms=5000` + a registered stats_cb (added to librdkafka_e2e_stats.c
so it actually computes/serializes stats). 30-min, default config, 4-min warmup, /proc CPU, 36p/200p.

## CONTAMINATION (read first)
Both boxes produced to the SAME cluster topic cmp-bench-200p; where ARM & Intel 200p runs OVERLAPPED
in time the topic got 2 producers -> 2x rate -> backlog. INVALID runs:
- ARM RDK-200p-stats: 300k (2x), p99 30 — invalid.
- Intel R-200p-metrics: ~225k, p99 32 — invalid.
Clean (non-overlapping): both Rust 36p runs (150k), Intel RDK-200p-stats (150k, ran after ARM finished).
LESSON: parallel boxes must use SEPARATE topics (or run sequentially). Fix next time.

## Clean results + cross-reference to earlier clean runs
| client/config | partn | CPU | p99 | RSS | source |
|---|---|---|---|---|---|
| Rust no-metrics | 36p | 102-104% | 17 | 28-33 | AA-36p / batch1 (earlier) |
| Rust WITH-metrics | 36p | 109-112% | 23-24* | 26-29 | this batch (both boxes) |
| Rust no-metrics | 200p | 110% | 20 | 34.5 | user ARM baseline |
| Rust WITH-metrics | 200p | 116% | 19 | 32 | metrics30_200p clean (earlier today) |
| librdkafka no-stats | 200p | 145% | ~19 | ~144 | batch1 Intel (single-poll) |
| librdkafka WITH-stats | 200p | 148% | 19 | 56 | this batch Intel (clean) |

## Conclusions
- **Rust metrics overhead: ~+6-9% CPU** (36p +5-8, 200p +6) — consistent with the 5-min check. Latency/RSS
  /throughput otherwise unaffected (200p clean run: p99 19 = no-metrics 20).
- **librdkafka statistics overhead: ~+3% CPU** (145->148) — much cheaper than Rust metrics, because
  librdkafka only SERIALIZES the stats JSON every interval (5s) whereas Rust/Java update windowed sensors
  per-record. So "metrics on" costs Rust ~6-9% but librdkafka ~3%.
- At 200p WITH their metrics on, Rust (116%) still beats librdkafka (148%) on CPU — single-poll dominates
  at high partition count regardless of stats.
- *CAVEAT: this batch's Rust 36p runs showed p99 23-24 vs the usual 17. The clean librdkafka run (later,
  20:24) was normal p99 19, so most likely shared-dev-cluster time-of-day load during the 19:00-20:24
  Rust window, NOT a metrics latency regression (the clean 200p metrics run earlier today was p99 19).
  Needs a controlled same-window no-metrics-vs-metrics 36p run to confirm.

Raw: metrics-batch-arm/, metrics-batch-intel/. Scripts: mbatch_arm.sh, mbatch_intel.sh. Harness:
librdkafka_e2e_stats.c (stats_cb added).

## librdkafka statistics overhead — COMPLETE (added 36p, clean run rdk36-stats/)
| librdkafka | no-stats | with-stats | delta |
|---|---|---|---|
| 36p  | 84%  | 85%  | +1% |
| 200p | 145% | 148% | +3% |
librdkafka 36p with-stats: 150k clean, p99 16, p99.9 30, RSS 51. So librdkafka statistics is ~free
(+1-3%, periodic JSON serialization) vs Rust metrics +6-9% (per-record windowed sensors) — ~3-6x cheaper.
