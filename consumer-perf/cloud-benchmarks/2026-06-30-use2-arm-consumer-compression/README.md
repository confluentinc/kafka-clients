# Consumer-with-compression — decompression cost: rust vs librdkafka vs java

**Date:** 2026-06-30. **Box:** AWS us-east-2 **m8g.4xlarge** (ARM Graviton). **Cluster:**
Confluent Cloud DEDICATED 12-CKU (`pkc-devcg1y1mn`), in-region. **Topic:** `cmp-bench-200p`
(200p, RF=3). **SASL_SSL, KIP-848.**
**Workload:** consumer started first (`--no-produce`); fed by 4× `kafka-producer-perf-test`
@ 150k msg/s total, 2 KB, `compression.type=<codec>`, `batch.size=1MB`, `linger.ms=5`
(large-batch feed → realistic compression ratios). 2-min warmup + 10-min measured.
Consumer fetch config: `fetch.min.bytes=1`, `max.partition.fetch.bytes=1MiB`,
`max.poll.records=500`. CRC: rust **on** (+metrics), librdkafka **off**, java **on**.
CPU = /proc, % of one core.

## Results (valid arms)

| codec | rust CPU | rust e2e avg/p99 | librdkafka CPU | rdk avg/p99 | java CPU | java avg/p99 |
|-------|---------:|-----------------:|---------------:|------------:|---------:|-------------:|
| none  | **38%**  | 14.0 / 30 ms      | 42%            | 11.7 / 22   | 64%      | 13.4 / 28    |
| zstd  | **48%**  | 10.5 / 21 ms      | 62%            | 9.7 / 17    | 73%      | 10.9 / 22    |
| lz4   | **44%**  | 13.4 / 28 ms      | 45%            | 11.4 / 21   | 73%      | 13.7 / 30    |

**snappy + gzip: NOT MEASURED** — the us-east-2 cluster's API key stopped authenticating
at ~12:06 UTC (`SaslAuthenticationException`); the unreserved dedicated cluster lapsed
after >1 day (reaper). Those 6 arms failed fast. (gzip-decompress was the key watch — still open.)

## Findings (decompression cost = ΔCPU vs none)
- **Rust:** none 38% → **zstd +10 (48%)**, **lz4 +6 (44%)**. Decompression adds modest CPU.
- **librdkafka:** none 42% → **zstd +20 (62%)**, lz4 +3 (45%). zstd-decompress notably more
  expensive here.
- **java:** none 64% → zstd +9 (73%), lz4 +9 (73%).
- **Rust is the most CPU-efficient on the consume side** (38–48%) vs librdkafka (42–62%) and
  java (64–73%) — at this 200p/150k operating point (≈228 records/poll → per-poll-light;
  see note below).
- Compression slightly **lowers** latency (bigger compressed batches arrive together).
- Decompression is far cheaper than compression: no consumer collapse on zstd/lz4 (vs the
  producer's zstd dip / gzip collapse). gzip-decompress unmeasured but expected functional.

## Important operating-point note
At 200p/150k this consumer baseline is **38% CPU**, not the ~110% seen in earlier 36p runs,
because CPU is **per-poll-bound** and the large-batch feed (`batch.size=1MB`,`linger.ms=5`)
yields ~228 records/poll → ~394k polls (vs ~16M polls / ~20 rec-poll at 36p or with a
trickle feed). So absolute CPU here is the low-poll-frequency operating point; the codec
**ΔCPU** is the comparable signal. See `2026-06-30-use1-arm-30min-crc-compare/` for the
records/poll analysis.

## Files
- `<arm>_<codec>.log` (none/zstd/lz4 × rust/rdk/java) + `*_cpu.csv`; `ccomp.log` timeline.
