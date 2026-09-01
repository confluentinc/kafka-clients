# Confluent Kafka Rust — Producer Performance Results

Six scenarios, seven producer clients each, run against a real Confluent
Cloud cluster (`pkc-devcoz07r9.us-east-1.aws.devel.cpdev.cloud`). Each
scenario folder contains the complete result folder for every client
(`results.json`, `run.log`/`console.log`, per-second `metrics.jsonl` (or
`rust-native.jsonl`), and a `graph.png` of that client's own latency/
throughput/RSS over the run) plus one `comparison.png` overlaying all
seven clients' p50, p99, and throughput on one chart.

## Scenarios

| Folder | Message size | Partitions | `batch.size` |
|---|---|---|---|
| `low_msg_size_200p_1MB_batch_size` | 1 KB | 200 | 1 MB, uniform |
| `low_msg_size_36p_1MB_batch_size` | 1 KB | 36 | 1 MB, uniform |
| `high_msg_size_36p_1MB_batch_size` | 100 KB | 36 | 1 MB, uniform |
| `high_msg_size_200p_1MB_batch_size` | 100 KB | 200 | 1 MB, uniform |
| `low_msg_size_36p_16KB_batch_size` | 1 KB | 36 | 16 KB (rust/java family) / 1 MB (librdkafka family) |
| `low_msg_size_200p_16KB_batch_size` | 1 KB | 200 | 16 KB (rust/java family) / 1 MB (librdkafka family) |

Common config across all scenarios: `acks=all`, `linger.ms=5`,
`enable.idempotence=false`, `compression.type=none`, `buffer.memory=32MB`,
`max.in.flight.requests.per.connection=5` (rust/java-backed clients) /
`1000` (librdkafka-backed clients), 300s warmup + 3600s measured per case.

Clients per scenario: `rust-native`, `librdkafka-c`, `java`,
`python-sync-rust`, `python-sync-librdkafka`, `python-async-rust`,
`python-async-librdkafka`.

## Substituted cases

The original runs of three cases showed a severe, later-found-to-be
non-reproducible memory/latency blowup (RSS reaching several GB past the
32MB `buffer.memory` cap, latency spiking to 10s+). Each was rerun on
2026-08-24 and came back healthy; **the rerun result is what's included
here**, in place of the original run:

- `low_msg_size_36p_16KB_batch_size` → `03-1kb-36p-1-rust-native`
- `low_msg_size_200p_16KB_batch_size` → `04-1kb-200p-3-java`
- `low_msg_size_200p_16KB_batch_size` → `04-1kb-200p-4-python-sync-rust`

The original (anomalous) runs of these three cases are not included in
this folder.

## Debug-timing logs (`*.log.gz`)

Two 1-hour runs at the `low_msg_size_200p_1MB_batch_size` config were
captured with per-stage `PERF_DEBUG_TIMING=1` instrumentation (nanosecond
timestamps at every hop of the send path — Python → C extension → Rust
FFI → Rust core, or just the Rust core for the native client), as part of
a separate root-cause investigation into elevated rust/java latency vs
librdkafka. These are not part of the 6-scenario comparison above, but
included for reference:

- `python-sync-debug-timing-first10min.log.gz` — first 10 minutes of the
  1-hour python-sync run (full file was 3.2 GB uncompressed / 526 MB
  gzipped — too large for a normal git push, so only an excerpt is
  included here).
- `rust-native-debug-timing-first8.5min.log.gz` — first ~8.5 minutes of
  the 1-hour rust-native run (full file was 8.5 GB uncompressed / 1.1 GB
  gzipped; trimmed to ~8.5 rather than 10 minutes so the compressed
  excerpt clears GitHub's 100MB per-file limit with a safety margin).

Tag format: `[TIMING][<layer>] ts_ns=<nanoseconds> tag=<stage> <fields>`,
where `<layer>` is `PY`, `C`, `RUST-FFI`, or `RUST-CORE`. Note `ts_ns` is
on a different monotonic epoch per layer (`C` uses `CLOCK_MONOTONIC` since
boot, `RUST-*` use an `Instant` captured at first use) — deltas *within*
one layer's tag family are meaningful; don't subtract a `C` timestamp from
a `RUST-*` one directly.

## Not included here

- The full (non-excerpted) debug-timing logs — see above.
- Setup/smoke-validation subdirectories (topic creation checks, one-off
  config smoke tests) that aren't one of the 7 canonical client cases per
  scenario.

## Regenerating graphs

`graph.png` and `comparison.png` were generated with `plot_metrics.py`.
Rerun it against this folder (or any future results folder) to regenerate
everything from scratch:

```
python3 plot_metrics.py "/path/to/producer-perf"
```
