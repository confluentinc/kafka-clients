# USE1 Intel/ARM consumer benchmarks — index

Cluster: CCloud dev `lkc-devc22rvqgy` / `pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud` (**36 brokers**),
SASL_SSL, KIP-848 (`group.protocol=consumer`). Topics `cmp-bench-{200p,36p,10p}`, 2 KB msgs, default
config (`fetch.min.bytes=1`, `max.poll.records=500`) unless noted. Boxes: Intel `44.210.18.177`,
ARM/Graviton `3.239.120.13` (both stop/rotate IP). Latency = e2e (`now − record.timestamp()`); CPU =
% of one core (`/proc` sampler `cpu.csv`, except consumer-perf self-report which reads 0% on aarch64);
RSS = consumer process VmRSS.

## For the low-partition / high-connection single-Selector CPU analysis (next session)
- **`cpu_vs_throughput_36p.csv`** — consolidated CPU vs throughput @36p (Intel+ARM, Rust/librdkafka/Java,
  CRC on/off, producer count noted). Headline: matched **CRC-off ARM**, Rust/librdkafka CPU ratio
  **widens as rate drops** — 300MB 1.19×, 200 1.44×, 100 1.56×, 50 1.59× (Rust 27% vs rdk 17% @50).
- **`arm-profile-50mbs/`** — the perf + strace profile of the worst cell (ARM, 50 MB/s, 36p):
  - `pr_flat.txt` / `pr_graph.txt` — perf CPU profile: **Selector::poll cycle ~45%** (poll_channel_reads/
    try_read 24-30%, rustls deframe 17%, recvfrom path 15-25%); syscall-entry ~25%; tokio park ~6-12%;
    **collect_fetch (record work) only 6.7%**.
  - `strace_rust.txt` — ~24k syscalls/s: 13k epoll_pwait, 21k futex, **136k recvfrom (27k EAGAIN)**.
  - `strace_rdk.txt` — librdkafka thread-per-broker blocks in `ppoll` (idle conns ≈0 CPU).
  - Mechanism: ONE selector reads across all ~36 broker conns per cycle (cost ∝ cycle-rate × conns,
    ~independent of data) vs librdkafka thread-per-broker (cost ∝ data). Rust ALSO beats Java (same
    single-Selector arch). See memory `consumer_lowrate_cpu_profile.md`.

## Datasets
| dir / file | box | topic | rates | producers | CRC | dur | notes |
|---|---|---|---|---|---|---|---|
| `batch1-1producer/` | Intel | 200p,36p | 300MB | 1 | rust/java on, rdk off | 30m | **clean 3-way headline** |
| `batch/` | Intel | 200p,36p | 300/200/100/50 | 4 | rust/java on, rdk off | 30/15m | RSS polluted by 4 producers; CPU/latency ok |
| `d2_*`,`d36_*` | Intel | 200p,36p | 300MB | 4(ext) | rust on | 5m | early clean latency/RSS |
| `dz_*` | Intel | 200p | 300MB | 4 | — | — | broker-disruption recovery test |
| `arm-batch/` | ARM | 36p | 300/200/100/50 | 1 | rust/java on, rdk off | 30/15m | **clean ARM sweep** |
| `crc-matrix/` | ARM | 36p | 300/200/100/50 | 1 | rust/java OFF, rdk ON | 15m | CRC-flipped (completes 2×2) |
| `arm-profile-50mbs/` | ARM | 36p | 50MB | 1 | — | — | **perf + strace** (CPU breakdown) |
| `regr2-clean-arm/` | ARM | 36p | 300MB | 1 | rust on | 5m | Phase-39/40 regression check (no regression) |
| `summary.csv` | both | 200p,36p | 300MB | mixed | mixed | — | first 3-way batch summary |
| `arm-crc-matrix-summary.csv` | ARM | 36p | all | 1 | full 2×2 | — | CRC matrix consolidated |
| `*_drive.sh` | — | — | — | — | — | — | the drive scripts (reproduce) |

## Key conclusions (see memory for detail)
1. Latency: Rust tied with librdkafka/Java (p99 16-19) everywhere; the "Intel fat tail" was environmental
   (degraded USW2 broker) + startup-measurement artifact — not a code bug. Tail fixes (bg-wakeup +
   await_wakeup/prefetch) were already in since June 8.
2. Memory: Rust lowest (~20-33 MB); the earlier 80-156 MB was a **4-producer harness artifact** (proven).
3. CPU: matched-CRC, Rust ≈ librdkafka at high rate; gap widens at low rate (single-Selector fixed cost).
   Rust's CRC ~free (+1-4%), librdkafka's costly (+3-19%). Rust beats Java throughout.
4. Closing the librdkafka low-rate gap = thread-per-broker = HIGH risk + violates single-Selector mandate
   (CLAUDE.md §8) → out. Design-intact levers (selectedKeys completion, rustls read batching) are modest
   and partly parked. Absolute cost ~0.1 core; latency unaffected.
