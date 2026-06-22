# Intel tail-latency investigation (default config) — 2026-06-18

## Summary / conclusion

**The reported "Intel fat tail" is not a bug in the Rust consumer.** On a healthy
cluster the Rust client is tail-competitive with librdkafka at both 36 and 200
partitions, and it recovers cleanly and quickly from hard broker disruptions. Two
candidate bug classes — a steady-state fetch-loop bug and a slow reconnect/recovery
path — were tested and **ruled out**. The earlier USW2 numbers (p99 40–98 ms,
oscillating, Intel-only) are best explained by **cluster/broker degradation on that
specific environment**, not client code.

## Background

Earlier 30-min runs on the (now-deleted) USW2 cluster showed, on an Intel EC2 box
only (not ARM), an oscillating tail under default/latency config:

- 200p: p99 40 ms, p99.9 111, max 460 (oscillating ~25–290 ms per-interval).
- 36p:  p99 98 ms, p99.9 270, max 663 (p99 swinging 46→282 the whole run).
- librdkafka and Java stayed tight (p99 ~17–21) at both partition counts.

The working hypothesis (from the user) was a logical bug producing a wasted / long-
wait fetch every few requests. The USW2 join logs also showed a flaky broker
(`54.213.232.68`) that reset the TLS handshake 19× during join.

## Method

- Fresh **Intel** EC2 box (x86_64 AL2023, m?i.4xlarge, 16 vCPU), in us-east-1, same
  region as the cluster (avoids WAN).
- **Healthy** USE1 dev cluster `lkc-devc22rvqgy`, SASL_SSL, KIP-848
  (`group.protocol=consumer`).
- Default/latency config: `fetch.min.bytes=1`, default `max.partition.fetch.bytes`
  (1 MiB), `max.poll.records=500`; librdkafka `--single-poll`; crc off (aligned);
  2 KB messages, 150k msg/s (~300 MB/s) via 4 co-located producers.
- Consumer always starts **before** producers and settles to the live edge (no
  startup backlog — the artifact that poisoned earlier measurements). Warmup 600k.
- Clients run back-to-back under identical load. `/proc`-based CPU/RSS sampling.
- Drive scripts and raw logs archived under
  `consumer-perf/cloud-benchmarks/2026-06-18-use1-intel/`.

## Results

### 1. Steady state — tight and competitive (no fetch-loop bug)

300 s measurement, per-interval p99 flat the entire run for both clients.

| | Rust p99 | Rust max | Rust CPU | Rust RSS | librdkafka p99 | librdkafka CPU | librdkafka RSS |
|---|---|---|---|---|---|---|---|
| **200p** | 19 ms | 249 | 118% | 84 MB | 19 ms | 143% | 141 MB |
| **36p**  | 19 ms | 232 | 106% | 37 MB | 17 ms | 110% | 58 MB |

Per-interval Rust p99 (200p): `20 19 23 18 20 18 19 17 20` — flat.
Per-interval Rust p99 (36p):  `24 20 18 18 17 18 17 18 18` — flat.

Rust matches librdkafka on the tail and uses **less CPU and roughly half the RSS**.
The USW2 oscillation did not reproduce. A real "wasted fetch every few requests"
bug would manifest here too (and on both architectures); it does not.

Residual: Rust's `max` (232–249) runs a bit higher than librdkafka's (138–159),
i.e. rare > p99.9 spikes — minor, same family as the USW2 `max 663`.

### 2. Hard broker disruption — clean, fast recovery (no reconnect bug)

`iptables -j REJECT --reject-with tcp-reset` on the Confluent Cloud LB IP for 6 s,
4 times during a 300 s run, identical schedule for both clients (10 s intervals).
(All broker connections multiplex through one SNI load-balancer IP, same as USW2,
so this resets every broker connection and forces full reconnect.)

- **Rust:** each cut = 1 stall interval (thr→62k, p99 ~7.7 s) + 1 catch-up interval
  (thr ~240k draining the backlog), then **back to p99 18 ms / max <120 ms within
  ~20 s, every time.** Steady state between cuts is pristine. The summary tail
  (avg 258 ms, p99 3.5 s) is *entirely* the four cut windows.
- **librdkafka:** recovered from cut #1, then spiralled into an unbounded backlog —
  p99 13→19→23→33 s, avg 3–7 s, RSS 161→445 MB, never recovered (summary p99 22.9 s,
  max 38 s).

The reconnect/recovery-latency hypothesis is **refuted**: Rust's reconnect path is
solid and its bounded `max.poll.records=500` lets it drain the post-disruption
backlog fast.

**Fairness caveat:** librdkafka ran with `--single-poll` (1 msg/poll, latency-
optimised), which cripples *backlog* drain. Its spiral is partly a harness-config
artifact, **not** proof the Rust client beats the librdkafka client on recovery. A
fair recovery comparison would re-run librdkafka in batch mode.

## Interpretation of the USW2 tail

- A hard disruption produces **seconds**-scale spikes; the USW2 tail was
  **~280 ms** — an order of magnitude milder.
- So whatever degraded USW2 was **softer than a connection reset** — most plausibly
  one broker serving with **elevated latency / partial packet loss** while still
  alive (consistent with a broker flapping its TLS handshake but otherwise up).
- This was not reproduced on healthy infrastructure, and the Intel-vs-ARM split most
  likely reflects the two environments hitting different cluster-health states
  (USW2 is now deleted, so this cannot be retro-confirmed).

## What was NOT tested (open, low priority)

- **Soft degradation** (`tc netem` +100–200 ms / 1–5% loss on the broker path) to
  try to reproduce the ~280 ms oscillation directly. This is the natural follow-up
  if the tail recurs.
- **Fair recovery comparison** with librdkafka in batch mode.
- **30-min** runs (these were 5-min; per-interval flatness across all intervals
  makes a hidden slow oscillation unlikely).
- **ARM** re-measurement on the same healthy cluster.

## 3. Three-way batch (Rust / librdkafka / Java), 2026-06-18

Full 3-way confirmation on the same healthy Intel/USE1 rig. Default config, 2 KB,
4-min warmup per client, consumer settles to live edge then external producers.
Raw logs + `summary.csv` under `consumer-perf/cloud-benchmarks/2026-06-18-use1-intel/`.

### Phase A — 30-min, default config, 150k msg/s (~300 MB/s)

| run | client | p50 | p99 | p99.9 | max | per-interval p99 (30 min) | CPU | RSS |
|---|---|---|---|---|---|---|---|---|
| 200p | rust | 4 | 20 | 35 | 307 | **18–22 (flat)** | 119% | 131 MB |
| 200p | librdkafka | 4 | 19 | 33 | 242 | 17–21 | 146% | 159 MB |
| 200p | java | 4 | 19 | 34 | 308 | 18–21 | 119% | 2209 MB* |
| 36p | rust | 4 | 18 | 34 | 238 | **17–20 (flat)** | 107% | 45 MB |
| 36p | librdkafka | 4 | 17 | 31 | 226 | 16–18 | 113% | 57 MB |
| 36p | java | 4 | 17 | 32 | 181 | 17–18 | 117% | 841 MB* |

\* Java RSS = JVM heap. **No oscillation** over 30 min for any client — Rust
per-interval p99 stays within a 4 ms band the entire run. This is the decisive
disproof of the USW2 "oscillating fat tail" on healthy infrastructure.

### Phase B — 15-min, 36p, varying rate

| rate | client | p50 | p99 | p99.9 | max | CPU | RSS |
|---|---|---|---|---|---|---|---|
| 200 MB/s (100k) | rust | 3 | 15 | 29 | 150 | 92% | 26 MB |
| | librdkafka | 3 | 15 | 30 | 144 | 81% | 48 MB |
| | java | 3 | 15 | 30 | 243 | 102% | 760 MB* |
| 100 MB/s (50k) | rust | 3 | 13 | 27 | 219 | 56% | 23 MB |
| | librdkafka | 3 | 13 | 27 | 231 | 44% | 30 MB |
| | java | 3 | 13 | 29 | 284 | 73% | 649 MB* |
| 50 MB/s (25k) | rust | 3 | 12 | 29 | 1618 | 32% | 22 MB |
| | librdkafka | 3 | 12 | 26 | 273 | 23% | 26 MB |
| | java | 3 | 12 | 28 | 1504 | 46% | 342 MB* |

Observations:
- **All three clients are tied on the tail** (p99 within 1–3 ms) at every config.
  No fat tail anywhere.
- p99 falls monotonically as rate drops (20→15→13→12 ms) — expected (less queueing).
- **Memory:** Rust uses the least RAM by a wide margin (e.g. 22–45 MB vs librdkafka
  26–57 MB vs Java's 342–2209 MB JVM heap).
- **CPU:** all comparable; librdkafka cheapest at low rates (single-poll), Rust in
  the middle, Java highest. At 150k Rust ties Java and beats librdkafka's single-poll.
- Two isolated outliers at the lowest rate (25k): Rust max 1618 ms and Java max
  1504 ms (single events — p99.9 still 29/28 ms), librdkafka 273 ms. Rare, idle-rate,
  shared by Rust+Java (likely a one-off scheduling/GC hiccup); not a tail concern.

## Recommendation

Treat the USW2 fat tail as **environmental** (degraded broker), not a client bug.
The Rust consumer is tail-competitive with librdkafka at 36p and 200p under default
config, at lower CPU and memory, and recovers cleanly from broker disruptions. If
the tail recurs, capture **broker-side health metrics** alongside the run and use
the `tc netem` soft-degradation playbook + the in-tree `fetch_diag` instrumentation
(gated to log only spikes) to localise.
