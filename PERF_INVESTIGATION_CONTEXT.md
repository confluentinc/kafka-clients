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

## SESSION 4 (2026-09-03): python-sync-rust producer throughput ceiling ROOT-CAUSED

Question: PRODUCER_RECORD_SLOT_THRESHOLD tuning (tested 1000/5000/32000, no
diff) — is a lower value (≈0) worth it, is the config needed? And separately,
why does python-sync-rust now measure ~16.7k msg/s when the org report showed
~134k?

Method: made THRESHOLD a compile-time override (#ifndef guard in
_confluentkafka.c), A/B'd threshold=1 vs 1000 same-window; added env-gated
PERF_SEND_TIMING to producer.py send() splitting FFI-call vs backpressure-
block cost; ran rust-native core in the same window as the disambiguator.
All on the S1 cell (max rate, 1KB, 200p, batch.size=1MB, acks=all, linger 5,
buffer.memory=32MB, none). Box restored to clean git source afterward.

Findings:
- **threshold is a no-op**: thresh=1 → 16,749 msg/s, thresh=1000 → 16,623;
  identical on every metric. Not worth exposing as a knob; keep as a fixed
  internal default (it does matter for faster/native callers).
- **The ~16.7k ceiling is the python binding layer, not the core and not the
  cluster.** rust-native core = **525,461 msg/s** in the SAME window (31x).
  So the gap is entirely the python send path.
- **PERF_SEND_TIMING localizes it to BACKPRESSURE, not FFI**: over a 180s run,
  sends=3,030,179, full_returns=3,030,179 (100% of sends hit the accumulation
  ceiling), backpressure_blocks=2,851,178 (94%), bp_avg=46.6us, ffi_avg=5.9us.
  Time parked in backpressure = 2.85M × 46.6us ≈ **133s of 180s (74%)**.
- **FINAL RESOLUTION (supersedes the two paragraphs below, kept for the
  audit trail): the 16k was a BUILD-SKEW ARTIFACT on the box, not a code
  regression.** The python ext links target/release/libconfluent_kafka.so;
  the box's copy was a STALE Aug-28 build (test-profile prebuilds never
  rebuild the cdylib) mismatched with the current source. Complete matrix,
  same window:
    | core .so            | gate  | msg/s   |
    | stale Aug-28        | 1000  | 16,623  |
    | stale Aug-28        | 1     | 16,749  |
    | stale Aug-28        | off   | 133,542 |
    | clean rebuild (ffi) | 1000  | 131,669 |  <- stock code = report's 134k
  With a coherent build the stock gate NEVER BINDS at these rates
  (accumulated stays <1000) and throughput is full. The stale .so had
  pathological drain-cycle latency that made the gate ping-pong; gate-off
  masked it by letting enqueue/drain pipeline. So: no committed-code
  regression; the gate + threshold are irrelevant in healthy builds; the
  remaining python-vs-core gap (132k vs 525k = ~4x) is the long-known
  GIL/binding cost. LESSON: the ffi cdylib is NOT rebuilt by cargo test
  prebuilds — always `cargo build --release --features ffi` before python
  perf runs, and check the .so mtime against the source state.
  FINAL numbers on canonical branch tip ba8d6366 (box synced via git
  bundle; includes Kaushik's PR #179 python at-ack callbacks + master
  merge), python-sync-rust max rate, AT-ACK latency, 10 min/arm:
  gate=1000: 148,521 msg/s, p50 30 / p95 41 / p99 52 / avg 31.2ms,
  CPU 139%, RSS 234MB; gate OFF: 149,064 / 30 / 42 / 51 / 31.8ms /
  145% / 223MB — identical; gate verdict confirmed on final code. Note
  +13% throughput vs the pre-sync build (PR #179 drain-stall fix
  d38dc1e7 + master merge) — python-sync-rust now exceeds the report's
  134k. Earlier python latency numbers in this section were reap-time
  (pre-PR-#179 harness); these at-ack figures supersede them.
  Full 2x2 matrix on branch tip ba8d6366 (at-ack, max rate, 10 min/arm),
  dispatch batching (PRODUCER_RECORD_SLOT_THRESHOLD) x backpressure gate
  (PRODUCER_MAX_ACCUMULATED_RECORDS, decoupled via #ifndef overrides):
    | batching  | gate | msg/s   | p50 | p95 | p99 | avg  | CPU% |
    | on (1000) | on   | 148,521 | 30  | 41  | 52  | 31.2 | 139  |
    | on (1000) | off  | 149,064 | 30  | 42  | 51  | 31.8 | 145  |
    | off (=1)  | on   | 115,280 | 23  | 36  | 52  | 24.4 | 158  |
    | off (=1)  | off  | 114,837 | 22  | 33  | 43  | 23.2 | 159  |
  VERDICT: dispatch batching = +29% throughput for +8ms latency (a real
  linger-like tradeoff); the gate = zero effect in every quadrant (pure
  memory-safety bound); removing both = the latency-optimal point
  (23.2ms avg / 43 p99 @ 115k).
  ALSO FOUND: the 1000/10ms staging is an UNCONTROLLED PRE-LINGER the
  user's linger.ms never sees — linger.ms=0 is not honored by the python
  binding (up to ~10ms staging at low rate, ~7ms at max rate; the ~8ms
  stock-vs-unbatched delta IS this staging cost). Java has no such layer.

  DESIGN RECOMMENDATION (user-agreed direction, needs its own
  actor/critic cycle + prototype measurement):
  1. Replace the threshold+10ms dispatch trigger with NATURAL BATCHING:
     signal the send thread on append-when-idle; each cycle drains
     everything accumulated. Batch size self-adapts (large at high rate,
     1 record at low rate); no constants; linger.ms semantics restored.
     NOTE the measured -23% for THRESHOLD=1 is a naive WORST CASE (per-
     append condvar signal, no batch growth); natural batching's true
     cost is between 0 and 23% — prototype to find out.
  2. Keep the space-wait but size the bound from the user's
     buffer.memory (bytes), not hardcoded 1000 records: then it fires
     exactly when Java's send() would block — Java-parity semantics,
     zero new configs, invisible when healthy (proven by the matrix).
  Net: no user-visible knobs added; linger.ms and buffer.memory start
  being honored properly by the python binding.
  Prior closing control (pre-sync clean core, gate ON vs OFF): 131,669 vs 130,509 msg/s,
  avg 33.7 vs 35.1ms, CPU 134 vs 138%, RSS 222 vs 236MB — identical within
  noise. In a healthy build the gate NEVER binds at python's ~132k enqueue
  rate, so it costs nothing; its role is purely protective (bounds C-side
  accumulation memory if the core drain stalls — Java-faithful send()
  blocking). Keep it. Only if a future faster binding pushes enqueue past
  the drain would tying the bound to buffer.memory instead of 1000 records
  (~1MB at 1KB) become worthwhile.
- **Mechanism (EMPIRICALLY PROVEN by a gate on/off A/B, not by git dates — 
  CORRECT observation, WRONG attribution; superseded above)**:
  the C extension's Python-side backpressure gate — send() returns "full" once
  accumulated_records >= PRODUCER_MAX_ACCUMULATED_RECORDS (= THRESHOLD = 1000)
  and the caller blocks on a space callback — forces a synchronous ping-pong
  (Python send → block → send-thread drains one ~47us cycle → unblock →
  repeat), serializing enqueue with drain. Decisive experiment (same binary,
  same window, only MAX_ACCUMULATED changed, guarded independently of the
  dispatch SLOT_THRESHOLD):
    - bound=1     → 16,749 msg/s
    - bound=1000  → 16,623 msg/s  (stock)
    - bound=100M (gate off) → **133,542 msg/s** (= the report's 134k), CPU
      unchanged 137%, RSS 196MB (safe — the Rust core's buffer.memory
      BufferPool still backpressures underneath).
  So it is the gate's PRESENCE, not its value, that caps throughput ~8x
  (that's why threshold=1 ≈ 1000: both ping-pong). rust-native core =
  525,461 msg/s in the same window, so the ceiling is purely this binding gate.
- **Git attribution — corrected**: the gate was ADDED in 70f3ec97 (author-
  dated June 24) but that alone did NOT prove causation, and the report binary
  72603114 (Aug 28) DOES contain the gate. The report's 134k is the Aug-23
  python-sync-rust row, explicitly "not re-run" — a PRE-gate binary; had
  72603114 been re-run it would have shown ~16k. The on/off experiment above,
  not the commit dates, is what establishes causation.
- **Real fix (binding code, validated by the experiment, NOT yet done)**:
  remove / loosen the hard Python-side ping-pong gate so the Rust core's
  buffer.memory is the backpressure authority (as in Java); the gate at any
  finite bound serializes send with drain. Separate from the harness; needs
  its own actor/critic cycle.

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
