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

---

## UPDATE (2026-06-08) — Root cause traced and primary fix landed

Followed up the "config is a workaround" concern by instrumenting the fetch
lifecycle (timestamps at fetch SEND, wire-arrival of the response, the bg-task
handling of the response, and `createFetchRequests` triggers) and running at
50k/s.

### What the trace showed
Steady state is a **burst of fetches with ~1ms round-trips, then a ~500ms gap
with no fetch activity**, repeating. In the gap:

```
t=...339  FT-WIRE   fetch response arrives on the socket (≈1ms RTT)
   … 498ms with the bg task parked in its network poll …
t=...837  FT-RECV   the response is finally handled by run_once
t=...837  FT-SEND   the next fetch is finally sent
```

The response was on the socket in ~1ms but was not **handled** (decoded into the
`FetchBuffer`) for ~500ms. Producer-side latency is negligible (0.5ms avg, 6ms
p99), so this is entirely consumer-side.

### Root cause (primary)
A fetch response is routed from the network client back to `AbstractFetch`
through a spawned forwarder → mpsc channel → `drain_pending_completions()` at the
top of the next `run_once`. Enqueuing that completion **did not wake the bg
task's network poll**. So when the consumer has caught up (no backlog keeping
`run_once` cycling), the bg task stays parked in `NetworkClientDelegate::poll`
for the full `poll_wait_time_ms` (= `maximumTimeToWait`, up to
`MAX_POLL_TIMEOUT_MS`) **after the response already arrived**, before the next
`run_once` drains it into the buffer. That parked window is the latency: records
produced during it age up to ~the poll/`fetch.max.wait` window. Java instead runs
the response handler **synchronously inside the poll** (`handleFetchSuccess →
fetchBuffer.add`), so data is buffered immediately.

This also explains why lowering `fetch.max.wait.ms` alone barely helped (the
broker returns in ~15ms once a fetch is sent — the problem was the *parked bg
task not draining the already-received response*, not the broker holding), and
why the app poll-timeout was the dominant lever (it sets `maximumTimeToWait`,
hence the park duration).

### Fix (landed)
`FetchRequestManager` now holds a clone of the bg task's `event_notify`
(`completion_notify`); each response forwarder pokes it right after enqueuing the
`PendingFetchCompletion`. This is the same wake the application-event enqueue
path uses — it makes the network poll return at a safe boundary so the next
`run_once` drains the completion within ~1 cycle instead of after
`maximumTimeToWait`. Measured WIRE→handle lag dropped from ~500ms (p99) to **p99
1ms, max 6ms**.

Result on **default config** (poll 500, fetch.max.wait 500), no config change:

| Rate | avg | p50 | p90 | p95 | p99 | p99.9 | (was avg / p99) |
|------|-----|-----|-----|-----|-----|-------|------------------|
| 50k  | 20  | 1   | 3   | 163 | 441 | 512   | (253 / 832)      |
| 100k | 19  | 1   | 5   | 136 | 432 | 513   | (275 / 894)      |
| 200k | 43  | 2   | 177 | 341 | 482 | 542   | (315 / 1289)     |

avg and p50 improve ~10–250× (p50 247→1ms). Full lib suite green (1709), clippy
clean.

### Residual tail (secondary, follow-up)
A residual ~450ms p99/p99.9 tail remains: ~6 rare large inter-fetch gaps per 30s
(378–500ms). Mechanism: when the consumer catches up and `poll()` returns empty,
**no fetch is left in flight** (the chain `poll-returns-records → send_prefetches
→ next fetch` breaks on an empty return), so newly produced records wait until
the next `poll()` call re-triggers a fetch. Java avoids this because
`pollForFetches` **blocks on `fetchBuffer.awaitWakeup(pollTimeout)` with the
per-poll fetch still in flight (long-polling)** — the broker holds that fetch and
wakes the consumer the instant data lands.

Recommended follow-up fix (more invasive — its own change + review): make the
Rust app `poll()` block on the `FetchBuffer` wakeup (like Java's `awaitWakeup`)
instead of spinning, and ensure a fetch is always in flight while waiting (issue
`createFetchRequests` whenever a fetchable node has no in-flight fetch). This
would close the residual tail and likely also help the throughput ceiling
(continuous prefetch overlap).

---

## UPDATE (2026-06-08, part 2) — residual tail fixed (Option 1: faithful `awaitWakeup`)

Dug into the residual tail. Found the Rust `poll()` had **dropped Java's
`fetchBuffer.awaitWakeup(pollTimeout)` block**: `poll_for_fetches` was a pure
sync `collect_fetch`, and the poll loop re-checked in a tight loop. Confirmed
empirically: at 1000 msg/s (idle) the consumer burned **103% CPU busy-spinning**.
`FetchBuffer::await_wakeup` existed (translated + unit-tested) but was never wired
into the poll path — an incomplete translation.

### Java vs Rust (the gap)
Java `pollForFetches`: `collectFetch()`; if empty, `fetchBuffer.awaitWakeup(
min(maximumTimeToWait, remaining))` **blocks** (woken by the bg `fetchBuffer.add`);
`collectFetch()` again. A fetch issued by the per-poll `AsyncPollEvent`
long-polls at the broker while the app blocks, so new data wakes the consumer
immediately. Rust had the first `collect` only.

### Fix (landed)
1. **Block like Java.** `poll_for_fetches` is now `async` and, when the first
   collect is empty, blocks on `fetch_buffer.await_wakeup(min(maximum_time_to_wait_ms,
   remaining))`, `select!`'d against the §11 wakeup token (the Rust equivalent of
   Java's `wakeupTrigger.setFetchAction(fetchBuffer)`), then collects again.
   Eliminates the busy-spin; data wakes the wait the instant `add` fires.
2. **Keep a fetch in flight.** Before blocking, call `send_prefetches()` so a
   fetch is always outstanding (long-polling) while we wait — `createFetchRequests`
   is a no-op for nodes that already have one. Closes the "caught up, no fetch in
   flight" gap that otherwise stranded the wait until `maximum_time_to_wait`.
3. **No-spin guard** (`FetchRequestManager::poll_internal`). When
   `prepare_fetch_requests` returns empty *because nodes already have an in-flight
   fetch*, do **not** `fetch_buffer.wakeup()` — a response is on the way. Only wake
   when the in-flight set is empty (genuinely nothing to fetch). Without this, the
   prefetch-before-block from (2) busy-looped (`wakeup` → `await_wakeup` returns →
   re-trigger → empty via in-flight skip → `wakeup` → …), which spiked idle CPU to
   140%. Two regression tests added
   (`test_poll_empty_with_inflight_does_not_wake_buffer`, `..._no_inflight_wakes_buffer`).

### Results (default config, no config change)

| Rate | avg | p50 | p95 | p99 | p99.9 | (orig avg / p99) |
|------|-----|-----|-----|-----|-------|-------------------|
| 50k  | 1.2 | 1   | 3   | 9   | 43    | (253 / 832)       |
| 100k | 1.4 | 1   | 4   | 12  | 90    | (275 / 894)       |
| 200k | 2.3 | 1   | 6   | 33  | 172   | (315 / 1289)      |

Idle CPU (1000 msg/s): **103% → ~4%**. avg ~150× better, p99 ~40–100× better,
stable across rates. Full lib suite 1711 passed; clippy clean.

The earlier UPDATE's framing ("config is the only lever") is now superseded: the
latency floor was two consumer bugs — the bg-not-woken stall (part 1) and the
dropped `awaitWakeup` + no-fetch-in-flight gap (part 2) — both fixed in code.
