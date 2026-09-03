# Producer perf investigation — full context handoff (updated 2026-09-01, session 2)

Continuity file for the producer performance investigation. Written so a
fresh session (or a person) can continue without the prior conversation.
Companion memory files (auto-loaded via MEMORY.md index):
`sendpath-libc-bucket-fixes`, `compression-codec-matrix`,
`producer-perf-latency-rootcause`, `producer-throughput-mysteries`,
`ec2-perf-benchmark-setup`, `tmp-worktree-reaper-hazard`.

## Where the work lives

- **Branch: `compression-perf-improvements`** (off `perf-harness-fixes`),
  NOT pushed. Worktree: `/private/tmp/claude-501/...-example-confluent-kafka-rust/
  49c7dc4c-*/scratchpad/phf` — NOTE: /tmp worktrees get reaped by macOS's
  midnight cleanup (see memory `tmp-worktree-reaper-hazard`); repair with
  `git worktree repair <path>` + `git checkout -- $(git ls-files -d)`.
  All commits are safe in the main repo's .git.
- Commits (in order): `4205559a` (harness instrumentation + rust-harness
  100ms-checkpoint pacing), `2b125053` (BufWriter + lz4 64KB block pin +
  flate2→zlib-rs), `7b02ed6c` (result_snapshot single-lock, thunks presize,
  lz4_flex safe-encode off), `34ed0e3a` (LZ4 encode → real liblz4 via lzzzz),
  `0193f121` (three per-batch memcpy/memset eliminations), `b4b67354`
  (fixup: 4 regression tests, Critic 67), **NEW `648b6232`** (env-gated
  batch-lifecycle timing instrumentation, PERF_DEBUG_TIMING=1 — supersedes
  the loose `producer-perf-ack-timing*.patch` files, whose src/ hunks are
  now committed; the tests/ hunks were already obsolete),
  **NEW `e46cf7d2`** (java harness: pace in 100ms checkpoints like C/Rust).
- EC2 box `ubuntu@3.91.64.73` (key `~/Downloads/test-rust-performance.pem`),
  repo at `~/perf-test/example-confluent-kafka-rust` — src/ + the two
  harness files match branch tip (rsync'd; box also carries the
  instrumentation as applied text, equivalent to 648b6232). Perf binary
  built with `RUSTFLAGS="-C force-frame-pointers=yes"
  CARGO_PROFILE_RELEASE_DEBUG=1`; java jar rebuilt with the pacing fix
  (2026-09-01 13:43). Cluster pkc-devc35y00m. This session's driver
  scripts: `~/perf-test/{latinv,satinv,valinv}_driver.sh`, analyzer
  `~/perf-test/analyze_latinv.py`, results in `~/perf-test/latency-inv/`.

## PUSHED 2026-09-02: harness fixes are on origin/perf-harness-fixes

The harness-only fixes were cherry-picked onto latest origin/perf-harness-fixes
(post-PR-#179, was 0dd9fce3) and PUSHED as 0dd9fce3..72842f95 (worktree:
`example-confluent-kafka-rust-phfix` next to the main repo):
  - f5983622 keep check.crcs=true for the Rust python consumer backend
  - 5495f0fe PERF_LRK_STATS + A/B knobs + RUST harness 100ms pacing
    (cherry-pick of 4205559a; python conflict vs upstream at-ack resolved)
  - 87e3d5fd JAVA harness 100ms pacing (cherry-pick of e46cf7d2)
  - 72842f95 PYTHON harness 100ms pacing (NEW — both sync and asyncio
    loops, covers the librdkafka AND rust python backends; the shared
    send loop had the same 1s-burst bug)
All four harnesses now pace identically (LIMIT_RPS/10 checkpoints).
Anyone re-running Group B must rebuild every harness from ≥72842f95;
python-sync(librdkafka)'s FT2/FT4 "could not sustain 100k" should be
re-tried — burst pacing was demanding ~1M msg/s instantaneous from a
GIL-bound loop. The client-side perf commits + PERF_DEBUG_TIMING
instrumentation remain only on compression-perf-improvements (unpushed).
`compression-perf-improvements` still needs a rebase onto the new base
(expect conflicts where 4205559a/e46cf7d2 duplicates meet, plus the
python at-ack code) before any merge.

## SESSION 3 (2026-09-02/03): overnight CONSUMER benchmark — all 7 clients, client defaults

14 cells, all rc=0: {100B @ 50k msg/s, 100KB @ 250 msg/s} x 7 consumer
clients, no compression, 36 partitions, pre-created topics, 3000 s
measured per cell, e2e latency (produce→consume, load via
kafka-producer-perf-test.sh), **pure client defaults** (rust/lrk: no
fetch flags; java: --max-poll-records 500 = client default because
JavaE2E's harness default is 2500; python: USE_DEFAULTS=True). Results
on the box in `~/perf-test/overnight-consumer/` and archived locally at
`~/Downloads/overnight-consumer-results.tgz` (also has the producer
results.json files from session 2).

### 100B @ 50k msg/s — e2e ms

| client | p50 | p99 | p999 | avg | max | CPU% | RSS MB |
|---|---|---|---|---|---|---|---|
| rust | 6 | 11 | 23 | 6.02 | 266 | 6.7 | 25 |
| librdkafka | 6 | 10 | 24 | 5.96 | 1571 | 4.7 | 36 |
| java | 6 | 11 | 25 | 6.06 | 1309 | 6.8 | 683 |
| py-sync(rust) | 6 | 11 | 23 | 6.29 | 304 | 17.6 | 49 |
| py-sync(lrk) | 27 | 47 | 51 | 27.07 | 463 | 9.4 | 71 |
| py-async(rust) | 6 | 11 | 26 | 6.34 | 728 | 19.6 | 49 |
| py-async(lrk) | 27 | 47 | 51 | 27.49 | 138 | 10.6 | 72 |

### 100KB @ 250 msg/s — e2e ms

| client | p50 | p99 | p999 | avg | max | CPU% | RSS MB |
|---|---|---|---|---|---|---|---|
| rust | 11 | 16 | 29 | 11.52 | 308 | 7.1 | 26 |
| librdkafka | 10 | 15 | 28 | 10.24 | 802 | 5.0 | 36 |
| java | 11 | 16 | 28 | 11.49 | 176 | 8.3 | 667 |
| py-sync(rust) | 12 | 16 | 29 | 11.71 | 754 | 11.8 | 49 |
| py-sync(lrk) | 114 | 213 | 217 | 113.88 | 767 | 6.1 | 82 |
| py-async(rust) | 12 | 16 | 28 | 11.70 | 1636 | 13.0 | 49 |
| py-async(lrk) | 114 | 214 | 218 | 114.50 | 1582 | 6.3 | 83 |

### Takeaways

- Sanity confirmed at both size extremes: every cell hit its exact
  target rate, no errors/stalls. Native latency parity: rust ≈ java ≈
  librdkafka (rust +1.2ms on avg at 100KB only). rust CPU ≈ java, ~2pts
  above lrk (same small unprofiled gap as the 1KB run); rust RSS is the
  smallest of all 7 clients (25-26 MB).
- **python(librdkafka) collapses under client defaults**: 27ms avg @
  100B and ~114ms @ 100KB (4-10x worse than everything else, sync AND
  async), while python(rust) tracks native latency at both sizes. The
  older reports masked this because the harness applied 4MiB fetch
  tuning to both backends. Defaults-vs-tuned is a real product story.
- Ops notes from the night: the box venv was missing pytest (python
  harness imports it at module level — now installed); an unquoted
  multi-word EXTRA_CONSUMER_ARGS in a sourced .env executes its second
  word (use embedded quotes); a repair driver re-ran the 6 affected
  cells (all rc=0).

## SESSION 2 RESULT: the latency question is ANSWERED

All runs: FT2 cell = 1KB, 200 partitions, batch.size 1MB, acks=all,
linger.ms=5, idempotence off, none codec, buffer.memory 32MB, paced
100k msg/s; 60s warmup + 240s measured, 2026-09-01, CC pkc-devc35y00m.

### Fixed rate 100k msg/s (the meaningful latency comparison)

| client | avg ms | p99 ms | CPU% |
|---|---|---|---|
| rust (branch HEAD) | 22.7 | 48 | 25.6 |
| java (pacing-fixed harness) | 21.7 | 44 | 19.5 |
| librdkafka | 36.4 | 71 | 23.8 |

**Rust ≈ java, both ~38% LOWER latency than librdkafka.** The report's
FT2 numbers (rust 51.5 / java 50.9 avg) had two causes, both now fixed:
1. rust+java harnesses paced once per LIMIT_RPS msgs = 1-second flat-out
   bursts (~200ms of saturation every second) while the C harness always
   paced at LIMIT_RPS/10 checkpoints. Java harness fixed in `e46cf7d2`
   (java went 53.0 → 21.7ms avg on the same binaries); rust had been fixed
   in `4205559a` but the report's 72603114 binaries predated it.
2. The report's rust binary predated the fix rounds.

Decomposition (PERF_DEBUG_TIMING batch lifecycle, 1-in-8 sampled):
rust total 22.8 = queue 6.5 (linger 5 + ~1.5 drain) + inflight 16.4
(wire+broker). librdkafka (PERF_LRK_STATS): int_latency 11.6 + outbuf
1.7 + rtt 20.6 ≈ 33.9. Rust's wire+broker leg BEATS lrk's rtt alone.
Batches are full (~1014 records ≈ 1MB, sticky partitioner works).
MAX_IN_FLIGHT=100 at fixed rate: no change (22.6ms) — in-flight cap
is not a factor at 100k.

### Saturation (max rate, acks=all, same cell, LIMIT_RPS=0)

| client | msg/s | avg ms | p99 | CPU% |
|---|---|---|---|---|
| rust | 519.5k | 60.3 | 167 | 183.6 |
| rust MAX_IN_FLIGHT=100 | 520.7k | 60.1 | 173 | 185.3 |
| librdkafka | 540.7k | 43.6 | 151 | 249.3 |

- Rust throughput 475k (report) → **519.5k** (+9.4% from the committed
  fix rounds); now −4% vs lrk at **26% less CPU** (2.83k vs 2.17k
  msg/s per CPU%).
- **Saturation latency is queue-capacity, not code speed** (Little's
  law): rust 60.3ms × 519.5k ≈ 31MB ≈ the full 32MB buffer.memory;
  lrk 43.6ms × 540.7k ≈ 23MB against the same 32MB bound
  (queue.buffering.max.kbytes). The app loop keeps the buffer full;
  latency = buffer / service rate. Lower saturation latency = smaller
  buffer, a knob not a defect. MIF=100 changing nothing confirms
  in-flight depth is not the ceiling either.
- Decomposition at saturation: rust queue 21.9 + inflight 39.5;
  lrk int_latency 11.1 + outbuf 3.6 + rtt 23.9. Rust's larger inflight
  leg partly reflects max.request.size=8MB requests (up to 8×1MB
  batches per request) vs lrk's ~1MB default message.max.bytes —
  bigger requests, fewer of them; a tuning asymmetry, not a defect.
- **buffer.memory=16MB confirmation (2026-09-01, 600s measured each,
  max rate, otherwise same cell):** rust 498.8k @ 30.7ms avg / p99 64 /
  CPU 178%; librdkafka 534.7k @ 24.9ms / p99 55 / CPU 253%; java 573.2k
  @ 27.8ms / p99 60 / CPU 99%. Halving the buffer halved rust's avg
  (60.3 → 30.7, predicted ≈31) at −4% throughput; occupancy: rust
  30.7ms×498.8k ≈ 15.3MB (96% of bound, and 181k BufferPool blocked-
  allocate lines show the pool pinned full), lrk ≈ 13.3MB (83%), java
  ≈ 15.9MB (full). Latency-at-saturation = buffer.memory/throughput is
  now confirmed end-to-end for all three clients; the residual rust-vs-
  lrk delta (~6ms) is queue occupancy (96% vs 83% full), because rust's
  enqueue path outruns its sender while lrk's producing thread is its
  own bottleneck.
- The report's Group C acks=1 lrk p50=14ms vs rust 57ms is the same
  phenomenon: lrk's bottleneck there was its single producing thread
  (queue stays shallow), rust's app loop outruns its sender (queue
  full). Report acks=1 cells were run with a temporary box-local patch;
  no ACKS env exists in any committed harness.

### What remains open (throughput/CPU, not latency)

- Absolute saturation throughput −4% vs lrk and −9% vs java (572k):
  known CPU-substrate residuals from session 1 flamegraphs — rustls
  plaintext ingest copy ~9%, glibc malloc ~8% (mimalloc experiment:
  +10% throughput, −34 CPU pts — ADOPTION DECISION STILL WITH USER),
  Arc<str> topic-name refcount traffic ~7.5%, tokio lock/spinlock ~9%.
- Java CPU measurement audit (89% at 571k — all threads?) never done.
- Java harness completion-drain: java's own p99 at FT2 (44) still
  carries Future.get drain in older cells; at-ack callback fix d9a09651
  is in for rust/java rows used here.

### Suggested next steps (if continuing)

1. Decide mimalloc (biggest single lever on the remaining CPU gap).
2. Arc<str> per-batch topic-handle experiment (Java-parity preserved).
3. If the org report is to be refreshed: re-run its long cells with
   branch HEAD + pacing-fixed java harness; expect rust to lead all
   latency columns at fixed rate and ~520k+ saturation.

### Standing constraints

- Java design parity is mandatory (CLAUDE.md); no architecture forks.
- Never push the branch. Pre-commit hook runs full verify (commit with
  run_in_background, timeout ≥ 20 min, `git commit -F <file>`,
  PIP_INDEX_URL=https://pypi.org/simple/ override on macOS).
- Critic reviews: spawn kafka-critic with a number (67 used), findings
  to COMMENTS.<N>.md, resolve → COMMENTS.DONE.<N>.md + fixup commit.
