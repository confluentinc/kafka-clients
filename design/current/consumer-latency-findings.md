# Consumer e2e latency findings (2026-06-06)

Follow-up to the read-bound throughput fix. Measured steady-state e2e latency,
CPU and RSS with the `consumer-perf` benchmark at **fixed, sustainable** rates
(no `--peak`), 1 KB records, 12 partitions, ~180 s windows. Broker: single
local Kafka 4.2, KIP-848 (`group.protocol=consumer`).

E2E latency = consume wall-clock − record `CreateTime` (same machine, so clock
is consistent). The consumer settles to the live edge before the producer
starts, so this is true produce→consume latency on fresh records.

## Sweep (default config: poll-timeout 500 ms, fetch.max.wait.ms 500)

| Rate    | avg | p50 | p95 | p99  | p99.9 | max  | CPU      | RSS         |
|---------|-----|-----|-----|------|-------|------|----------|-------------|
| 50k/s   | 253 | 247 | 516 | 832  | 991   | 1022 | ~108%    | 190–256 MB  |
| 100k/s  | 275 | 263 | 601 | 894  | 999   | 1146 | ~114%    | ~same       |
| 200k/s  | 315 | 290 | 746 | 1289 | 2224  | 2521 | ~118–125%| 194–268 MB  |

(latency in ms; CPU as % of one core; RSS resident set)

- **CPU** ~1.0–1.25 cores across the range, scaling mildly with throughput.
- **RSS** stable ~190–270 MB over each 3-min run — no growth / no leak.
- The latency floor (~250 ms avg, ~515 ms p99) is **flat from 50k→100k** and only
  the *tail* grows toward 200k (queueing as we approach the ~280k/s ceiling).

## Root cause: the floor is the wait-timer config, not consumer compute

Producer-side latency is negligible — running `kafka-producer-perf-test` alone at
50k/s reports **avg 0.55 ms, p50 0, p95 3, p99 6, p99.9 16 ms**. So the entire
~250 ms avg / ~832 ms p99 is **consumer-side**.

A/B at 50k/s, varying the two 500 ms timers:

| Config (50k/s)                          | avg | p50 | p99 | max | polls/s | recs/poll |
|-----------------------------------------|-----|-----|-----|-----|---------|-----------|
| poll 500, fetch.max.wait 500 (default)  | 253 | 247 | 832 | 1022| 181     | 277       |
| poll 500, fetch.max.wait **50**         | 181 | —   | 515 | 631 | 367     | 136       |
| poll **50**, fetch.max.wait **50**      | **5**| **1**| **51**| 92 | 890     | 56        |

Lowering both timers to 50 ms cuts **p99 832 → 51 ms (16×)** and avg 253 → 5 ms
**at the same 50k/s throughput**, with CPU still ~1 core. So the latency floor is
dominated by the wait timers, not by per-record work (loop breakdown:
`per-record processing = 0.0%`, `poll().await = 100%` in every run).

### The subtle part — it is NOT poll batching

Polls are frequent in every config (181–890 polls/s; every ~1–5 ms), each
returning a modest batch — the app `poll()` is **not** sitting on its timeout
accumulating records. Yet at poll-timeout 500 the records are already ~250 ms old
**when the app drains them**. So they age *upstream of the app buffer* — between
a record becoming available at the broker and the **background task issuing/
completing the fetch that pulls it**.

With `fetch.min.bytes=1` the broker returns a fetch as soon as one byte is
available, and with `fetch.max.wait.ms=50` within 50 ms — yet the fetched data
still arrives ~180 ms old (A/B1). The decisive lever was the **app poll-timeout**
(A/B2: 515 → 51 ms), which means the background fetch cadence is **coupled to the
application's requested poll timeout** rather than running as a continuous,
independent prefetch. Longer poll timeout ⇒ the bg task issues fetches less
eagerly ⇒ each fetch's data is staler when it lands.

## Implications

- **For latency-sensitive use today:** poll with a short timeout and set
  `fetch.max.wait.ms` low (e.g. 50 ms). That yields p99 ≈ 50 ms at 50k/s here.
  This is a config choice, not a code change — the consumer is not CPU/alloc
  bound at these rates.
- **Follow-up (separate from this work):** the coupling between the bg task's
  fetch cadence and the app poll timeout is the latency analogue of the
  throughput "no prefetch overlap" gap (see consumer-throughput-bottleneck.md
  UPDATE 5). Java keeps a fetch continuously in flight per node (proactive
  prefetch) so data is usually already buffered when `poll()` is called, making
  latency largely independent of the poll timeout. Worth investigating whether
  the Rust bg task should issue the next fetch as soon as a node's buffered data
  is consumed, independent of the app poll timeout — this would likely improve
  *both* the default-config latency floor AND the throughput ceiling.
- Not yet compared head-to-head with the Java client at identical config; that
  comparison would confirm whether the coupling is a behavioral gap vs Java or
  inherent to the workload.

## Reproduce

```
cargo build -p consumer-perf --release
# default config sweep:
./target/release/consumer-perf --kafka-bin <kafka>/bin \
    --throughput 50000 --duration 180 --message-size 1024 --partitions 12
# low-latency config (same throughput):
./target/release/consumer-perf --kafka-bin <kafka>/bin \
    --throughput 50000 --duration 60 --message-size 1024 --partitions 12 \
    --poll-timeout-ms 50
# (the fetch.max.wait.ms A/B required temporarily lowering the ConsumerConfig
#  default; there is no CLI flag for it yet.)
```
