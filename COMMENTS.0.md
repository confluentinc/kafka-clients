# Critic 0 — re-review of the fix commits (`af60796`, `bdb8b77`, `8a30e27`, `3002047`, `7b913ea`, `9957c2b`, `c824fc6`)

Scope: correctness of the fixes themselves and regressions they may have
introduced. 5 issues found (2 Bug / Behavior Mismatch worth fixing, 3 lower
severity). The verification list of everything that checked out clean is at the
bottom.

**All 5 issues are resolved** — each original comment plus its resolution is in
`COMMENTS.DONE.0.md`. Fix commits: `f7e5eff` (issue 1) and `b53c941` (issue 2),
both `fixup! 7b913ea`; `62ea49e` (issue 3), `fixup! 3002047`; and the
`fixup! af60796` commit for issues 4-5.

---

## Verified clean (no issue raised)

**`7b913ea` — `AppendError` / callback hand-back**
- Callback-`Some`/`None` split is sound: `AppendError` is constructed at exactly
  two places (`try_append`'s `closed` check, before any batch is touched; the
  `free.allocate` failure, where the callback was just handed back by the
  previous `try_append`). No path exists where a batch took ownership and an
  `AppendError` is still returned, so no `None`-with-lost-callback case.
- **No double-fire**: `append` never invokes the callback itself (pinned by both
  new accumulator tests); `do_send_bytes` fires it once via
  `handle_api_exception`; the FFI `send_with_callback` (`ffi/producer.rs:1148-1163`)
  and `send_batch_async` (`:1501`) attach `make_record_callback` as the *only*
  callback and fire nothing extra on the `Ok(failed-future)` path, so the C
  callback runs exactly once.
- **No double-free**: the `append_new_batch` exit `buffer.take()`s before the
  move, and both new `if let Some(b) = buffer.take()` guards are no-ops once the
  buffer has been handed to a batch or already deallocated.
- Manual `Debug` (error + `callback_returned: bool`) keeps all `.unwrap()` /
  `.expect()` call sites compiling and hides nothing asserted on; `RecordAppendResult`
  is still not `Debug`, which is why the new tests `match` instead of `unwrap_err`.
- **All `append` callers updated**: the only production caller is
  `kafka_producer.rs:589`; the remaining 40+ sites are `#[cfg(test)]` in
  `record_accumulator.rs` / `sender.rs`.
- `test_callback_invoked_on_buffer_exhaustion` asserts both handles, the
  `INVALID_OFFSET` placeholder, and the future's `BufferExhausted` — a real
  exactly-once assertion, not `>= 1`.

**`af60796` — shared close-fn builder**
- Production ctor (`:2328-2331`), `spawn_dedicated_bg` (`:10083`) and
  `make_test_consumer_with_channels` (`:5774-5786`) all obtain both closures
  from `build_network_thread_close_fns`; the fixture's only additions are the
  observability flags (and a fixture-local `running` flag, which is inert
  because that fixture has no real bg loop). The divergence that hid Critic-3
  Issue 1 cannot recur.
- No production site fires the `WakeupTrigger` for an internal wake any more:
  the only two remaining `wakeup_trigger.wakeup()` calls are
  `AsyncKafkaConsumer::wakeup` (`:2715`) and `ConsumerHandle::wakeup` (`:233`),
  both user-facing. `ConsumerNetworkThread::{wakeup, signal_close}` are now
  delegate-/Notify-only and still have no production callers.
- The removed `wakeup: WakeupTrigger` field is genuinely dead: `wakeup` now
  appears only as the ctor parameter (`consumer_network_thread.rs:289`) used to
  derive `wakeup_rx`.
- `close_handle_wakes_bg_task_after_wakeup_trigger_disabled` is bounded on both
  sides (5 s `tokio::time::timeout` + `elapsed < 2 s` against
  `MAX_POLL_TIMEOUT_MS = 5000`) and the 200 ms pre-sleep only makes the test
  stricter, not racier — `Notify::notify_one` stores a permit, so a poke landing
  before the park is not lost. `await_bg_parked` is a bounded 1 s spin with an
  explicit panic message. Not flaky.
- Poking `event_notify` has no side effect beyond waking: the `run_once` arm
  only calls `network_wakeup.notify_one()` and sets `poked`; a spurious permit
  costs one early poll return.
- No test asserts "`app_event_notify` was not poked" after a path that now pokes
  it (only the pre-condition check at `:7123-7129`).

**`3002047` / `8a30e27` — listener contract**
- The `None`-overwrite is Java-faithful: `AsyncKafkaConsumer.java:2022-2023`
  → `subscribeInternal(topics, Optional.empty())` →
  `SubscriptionState.java:192-196` `registerRebalanceListener(Optional.empty())`.
  `SubscriptionState::register_rebalance_listener` (`:607-614`) assigns the
  `Option` verbatim, so app-side mirror and bg-side slot agree on all three
  subscribe paths.
- `subscribe_internal_topics` / `_pattern` / `subscribe_to_regex` are reached
  only from the six public subscribe methods — no internal caller can
  accidentally clear the listener.
- The empty-topics arm returns before `listener_for_app_side` is built, so the
  Arc really is dropped (release observed) — consistent with the new C test.
- `MockConsumer::subscribe_with_listener` (`mock_consumer.rs:483-493`) matches
  `MockConsumer.java:196-200` (no empty short-circuit, registers before the
  type check), so the mock-vs-real asymmetry the docs describe is Java's.
- All 23 C tests in `test_consumer_callbacks.c` compiled and passed here
  (direct `cc` recipe, `libconfluent_kafka.a` from `--release --features ffi`),
  including the three new arms and the two real-consumer fixtures — no broker
  contacted, no hang.

**`bdb8b77` — harness**
- `LogState` is now `unique_ptr` in both services, `Close` neither erases nor
  frees, and both `log_state_for` callers (`server.cc:474` Send, `:894`
  CommitAsync, `:815` Subscribe) are already behind a client-exists check, so
  the now-non-null-after-Close return cannot be used on a destroyed client. Ids
  are monotonic (`next_id_`), so a retained entry cannot collide with a new
  client.
- `wait_for_kind_settled` is correct and bounded (first-match wait, then a flat
  `grace` of re-reads, returning the last snapshot); it does not extend on
  activity, and the zero-match case still fails the `== 1` assertion loudly.
  `poll_until_kind` correctly left alone.

**`9957c2b` / `c824fc6` — Python**
- `kafka_consumer_Consumer_seek_with_metadata_async` is a faithful mirror of its
  sync sibling (same `metadata == NULL → String::new()`, same `leader_epoch < 0
  → None`, same inline-callback error path for a
  `OffsetAndMetadata::with_leader_epoch` failure) and error content is identical
  to the removed sync path — both surface whatever `c.seek_with_metadata(...)`
  returns, so no message-parity regression.
- The removed `py_Consumer_seek` / `py_Consumer_seek_with_metadata` have no
  remaining references: method table updated, `grpc_server.py:362-364` uses the
  sync `Consumer.seek`, `grpc_server_async.py` now awaits, `ConsumerHandle.seek`
  is a separate FFI family. The new `Consumer.seek` additionally calls
  `_check_closed()`, which is *more* Java-faithful (Java's `seek` on a closed
  consumer throws), and no test asserted the old behavior.
- Coroutine rejection happens in `_CommitCallbackAdapter.__init__`, i.e. before
  `_lib.Consumer_commit_async(...)` is called — no half-committed state, and
  `test_coroutine_commit_callback_is_rejected_on_the_sync_consumer` asserts
  `committed([tp]) == {}` for both overloads.
- `offsets_to_arrays`: the `PyUnicode_Check` pre-check runs before any pointer
  is stored, `ok = 0` reaches the existing `if (!ok) { offset_arrays_free(out);
  return -1; }`, and all four callers already bail on `n < 0` before taking a
  reference. No leak, no leftover exception indicator.
- `c824fc6`: no Python C-API call sits inside either
  `Py_BEGIN_ALLOW_THREADS` region — `py_Producer_flush` releases around the
  bare `kafka_producer_Producer_flush` and builds its `PyLong` after
  `Py_END_ALLOW_THREADS`; `py_Producer_partitions_for` was restructured to
  declare `err` first precisely so `Py_BuildValue` stays outside. Correct.
- `AsyncConsumer._loop` caching: `_run_async` refreshes it on every awaited op
  and `_listener_loop` refreshes it whenever a loop is running, so the cache is
  only consulted from a worker thread — the documented usage. A consumer driven
  from two different event loops could observe a stale loop, but that requires
  reusing one consumer across `asyncio.run(...)` calls; not raised.
- No contradictory assertions across the three actors' test additions (Actor 1's
  C suite, Actor 2's Python suite and Actor 3's server.cc/harness changes touch
  disjoint surfaces).

**Build/test**: `cargo test --features ffi --lib` → 2228 passed / 0 failed / 1
ignored, including `close_handle_wakes_bg_task_after_wakeup_trigger_disabled`
and the three new producer/accumulator tests.

---

# Investigation: `python-soak-ht` RSS spikes during broker rolls

**Status:** diagnosis only. Nothing has been changed. Reviewed-before-fix, per the
investigation brief.

**Date:** 2026-08-14
**Branch:** `dev/python-soak-client` @ `7d65a793` (post-rebase; Rust client and
bindings rebuilt from this HEAD for every measurement below)

---

## 1. Verdict

The spikes are caused by the **Python C extension**
(`bindings/python/_confluentkafka.c`), not by the Rust client and not by the soak
script.

Two defects compound:

1. **`BatchNode` is a fixed 44,016-byte (43.0 KiB) struct that is allocated per
   *drain cycle*, which at soak rates means roughly one per record** — five
   parallel `[1100]` pointer arrays (`producer_records`, `producer_structs`,
   `complete_cbs`, `futures`, `batch_errors`) are allocated in full whether the
   node ends up holding 1 record or 1100. At 80 msg/s against a 10 ms drain loop,
   ~1.16 records share each node. Measured: **43 KiB of bookkeeping per
   outstanding 10 KiB record — 4.3x the payload it describes.**

2. **The pending-batch list that holds those nodes is unbounded and drains
   strictly in order, so one slow record pins every node behind it.**
   `Producer_poll_futures_thread` (`_confluentkafka.c:342`) processes one
   `BatchNode` at a time, blocking in `FutureRecordMetadata_get_all` on that
   node's futures, and only calls `PyMem_RawFree(batch_to_free)` after the whole
   node completes. A single record retrying to an unresponsive broker (up to
   `delivery.timeout.ms`, default 120 s) therefore holds **every** later node
   resident — including nodes whose records were acknowledged long ago.

The advertised backpressure does not stop this. `PRODUCER_RECORD_SLOT_THRESHOLD`
= 1000 bounds only `accumulated_records`, the *pre-drain staging* counter, and
the send task resets it to 0 on every drain (`_confluentkafka.c:436`) — roughly
every 10 ms. So `producer.send()` never blocks, and the number of outstanding
records is bounded by nothing.

This also resolves the open question from the earlier local test: `outq` reaching
**6536 against a "1000-record" bound** is not a contradiction. `self.outstanding`
counts sent-but-undelivered records; `accumulated_records` counts records staged
for the next 10 ms drain. They measure different things, and only the latter is
capped.

The non-returning baseline is **glibc allocator retention, not a leak** — proven
in §5.

---

## 2. Reproduction

3-broker KRaft cluster (`apache/kafka:4.2.0`, RF=3, 6 partitions), soak run with
`HI=true`, `SOAK_PARTITIONS=6`, rate 80 msg/s, 10240 B payload.

| Scenario | Baseline | Peak | Settles at |
|---|---|---|---|
| Steady state | 52 MiB | — | 52 MiB |
| **Graceful roll**, all 3 brokers one at a time | 52 MiB | 54 MiB | 54 MiB |
| One broker hung 90 s (`docker pause`) | 54 MiB | 95 MiB | **96 MiB** |
| Two brokers hung 150 s | 96 MiB | 425 MiB | **425 MiB** |

Two things to take from this table.

**A graceful roll does not reproduce it.** Controlled shutdown moves leadership
before the socket closes, the producer retries to the new leader immediately, and
`outq` never exceeded 2. This is worth knowing: if production rolls are graceful,
the roll is a *correlate*, not the cause — the trigger is a broker that stops
answering while the connection stays open (frozen pod, node pressure, long GC, or
a roll moving faster than ISR recovers, which makes `acks=all` retry).

**The baseline ratchets exactly as reported.** 52 → 96 → 425 MiB, each spike
leaving a permanently higher floor, matching the Grafana shape.

---

## 3. Python vs native attribution

`tracemalloc` tracked RSS almost 1:1 throughout. That is initially
counter-intuitive — but it is the correct reading here, because the C extension
allocates via `PyMem_RawMalloc`, which **is** a tracemalloc-traced domain. So
"tracemalloc is climbing" does *not* mean "Python objects"; it means the traced
allocator, which includes the extension.

The top-sites snapshot at peak makes the attribution unambiguous. Producer-only
process, 150 s stall, 9436 outstanding records:

```
341.4 MiB   8132 blocks   /w/bindings/python/producer.py:258     <- Producer_send -> BatchNode
 92.4 MiB   9436 blocks   payload bytes (one per outstanding record)
  6.8 MiB  18876 blocks   threading.py:258
  1.7 MiB  16193 blocks   producer.py:230
```

`341.4 MiB / 8132 blocks = 44,032 bytes per block`. `sizeof(BatchNode)` compiled
on this platform is **44,016 bytes**. That is the struct, one block per node, and
it is the single largest consumer of memory in the process by a factor of 3.7
over the actual message payload.

---

## 4. Which component — the bisect

All runs: producer only, no consumer in the process, 150 s stall, 80 msg/s.

| | HI (10240 B, lz4) | non-HI (50 B, no compression) |
|---|---|---|
| Baseline RSS | 24.5 MiB | 24.5 MiB |
| Peak RSS | 422.5 MiB | 311 MiB |
| Peak tracemalloc | 452.6 MiB | 352.8 MiB |
| **`BatchNode` (producer.py:258)** | **341.4 MiB / 8132 blocks** | **339.3 MiB / 8084 blocks** |
| Payload bytes | 92.4 MiB | 0.7 MiB |
| RSS per outstanding record | ~40 KiB | ~30 KiB |

Shrinking the payload by 200x barely moves the spike: the `BatchNode` term is
**identical** (339 vs 341 MiB). The 10 KB payload contributes ~21% of the HI
spike; the fixed 43 KiB struct contributes the rest.

**The HI fetch tuning is not implicated, and I tested it at its strongest.** A
consumer-only process with `fetch.max.bytes=52428800` and
`max.partition.fetch.bytes=10485760` draining a 17,365-record (~170 MB) backlog,
with both brokers hung for 60 s mid-drain:

```
baseline 25.8 MiB -> peak 62.1 MiB (+36 MiB), flat across the outage, no growth after
```

`fetch.max.bytes` is a ceiling on what a broker may return, not a preallocation,
and the fetch path is already bounded correctly on the Rust side: the
buffered-partition and pending-node exclusions in
`src/consumer/internals/abstract_fetch.rs` (lines ~690, ~707-711, ~820) keep it
to one in-flight plus one buffered fetch per node, and `FetchBuffer::retain_all`
is called on every assignment change. **The leading hypothesis is refuted.**

The Rust producer is also not implicated: `buffer.memory` (32 MiB) and the
`BufferPool` bound the accumulator correctly. The reason that bound does not
bound *record count* is that `MemoryRecordsBuilder` compresses as it appends, and
the soak's payload (`b" SoakRecord nr #0"` repeated) is near-perfectly
compressible — so 32 MiB of accumulator holds effectively unlimited records. That
is correct Java-faithful behaviour; it just means the accumulator provides no
backstop for the C extension's unbounded pending list.

---

## 5. Leak or allocator retention?

**Allocator retention.** Decisive evidence from the HI run, after the stall ended
and every record drained:

```
# drained: outstanding=0  rss=404.2 MiB
# idle+10s ... idle+60s   rss=404.2 MiB   tracemalloc=0.63 MiB
# malloc_trim() -> 1 ; rss 404.2 -> 102.6 MiB (released 301.6 MiB)
# baseline was 24.5 MiB
```

`tracemalloc` fell to 0.63 MiB — the application freed everything — while RSS sat
at 404.2 MiB through 60 s of idle. `malloc_trim(0)` then handed 301.6 MiB back to
the kernel. Nothing is leaked; glibc is holding freed 43 KiB chunks. They sit
below the 128 KiB `M_MMAP_THRESHOLD`, so they come from the heap/arenas rather
than `mmap`, and glibc can only return contiguous free space at the top of a
heap. The same result in the non-HI run (310.3 → 136.4 MiB).

Note the trim did **not** return to baseline (102.6 and 136.4 MiB vs 24.5 MiB).
Residual fragmentation survives even an explicit trim — and production never
calls one. That is the ratchet.

So the graph is alarming but the process is not on a path to OOM from a leak. It
*is* a real defect: a stalled broker inflates RSS by ~40 KiB per in-flight record
and the memory is never given back.

---

## 6. Honest gap

I reproduced ~398 MiB of growth (24.5 → 422.5 MiB) against production's ~620 MiB
(170 → 790 MiB). The *shape* matches exactly — linear in outstanding records at
~40 KiB each, non-returning baseline, repeated spikes ratcheting — but the
magnitude is ~65% of production's largest spike.

At 80 msg/s and `delivery.timeout.ms=120000`, a single stalled batch can pin at
most ~9,600 records ≈ 400 MiB, which is what I measured. Production's 620 MiB
implies ~15,000 pinned records. Candidate explanations, in rough order of
likelihood, none yet tested:

- Overlapping stalls: a multi-broker roll re-stalls the head of the queue before
  it drains, so nodes from successive outages coexist.
- Production message rate or partition count differs from the 80 msg/s / 6
  partitions I ran.
- Greater glibc arena inflation under production's thread count.
- OTEL exporter buffering during the outage (my repro ran JSONL-only).

**What would settle it:** the production `producer.outq` series at the spike. If
it peaked near 15,000, the mechanism is fully confirmed and only the stall
duration differs. That series is already emitted — `set_gauge("producer.outq",
outstanding)` in `soakclient.py:1856` — so it should be in Grafana alongside
`memory.rss`. Overlaying the two is a five-minute check that closes this gap
without another soak run.

---

## 7. Proposed direction (NOT applied)

Flagging one change that looks safe and clearly correct, per the brief's
"propose, don't apply". It touches only the C extension, not the Rust send path
and not the fetch tuning, so it does not affect the running batch's client
behaviour:

**Make `BatchNode` hold its arrays out-of-line, sized to actual occupancy**, or
equivalently reduce `PRODUCER_RECORD_SLOT_CAPACITY` and allocate the node's
arrays after the drain count is known. A node holding 1 record would then cost
~100 bytes instead of 43 KiB, cutting the spike by ~97% at 50 B payloads and
~79% at 10 KB — without changing any semantics.

Two further items that need design discussion rather than a patch, because they
change blocking behaviour:

- The backpressure gate should count *outstanding* records, not staged ones, if
  it is meant to do what its comment claims ("mirrors Java's `send()` blocking
  once `buffer.memory` is full"). As written it cannot.
- The strictly-ordered pending-batch drain means head-of-line blocking is
  structural. Completing nodes out of order, or per-record rather than per-node,
  would remove the amplification where one stalled record pins thousands of
  delivered ones.

I have deliberately not written any of this. Recommend reviewing the diagnosis
first.

---

## 8. Reproducing this

Artifacts are in the session scratchpad (`compose.yml`, `exp_stall.py`,
`exp_consumer.py`, `run_exp.sh`, `sampler.py`) and the raw series in
`out/sample-soak-roll.csv`, `out/exp-hi.csv`, `out/exp-nohi.csv`. The method:

1. 3-broker KRaft cluster, RF=3, 6 partitions.
2. `HI=true` soak against it; let RSS settle.
3. `docker pause` one or two brokers — **not** `docker stop`; a graceful stop is
   absorbed and reproduces nothing.
4. Sample RSS, `tracemalloc`, and `outq` together; take a
   `tracemalloc.take_snapshot()` at peak outstanding.
5. After full recovery, call `malloc_trim(0)` via `ctypes` to separate retention
   from a leak.

---

# Part 2: Fix A implemented and measured

**Commit:** `e2350f86` on **`fix/python-binding-batchnode-memory`** (branched from
`origin/master` `e0f8165e`, not from this soak branch). Verified on a local
throwaway merge of that branch with this one; that merge was not pushed.

## 9. What changed

`BatchNode`'s five per-record arrays moved out-of-line into one allocation
carved into pointer-sized sub-arrays, starting at 8 slots and doubling to the
**unchanged** `PRODUCER_RECORD_SLOT_CAPACITY` chaining boundary. A node holding
one record costs ~392 bytes instead of 44,016.

Node boundaries, drain ordering, completion granularity, the
`PRODUCER_RECORD_SLOT_THRESHOLD` backpressure gate and every public surface are
untouched. Growth allocates a new block and relocates the live entries rather
than reallocating, because changing the capacity moves every sub-array's offset;
it runs at most a handful of times per node and never at soak rates. Allocation
failure now raises `MemoryError` with the record's references released, where
the previous unchecked `malloc` would have dereferenced NULL.

## 10. Results

Controlled producer-only stall (`exp_stall.py`, 10 KB lz4, 150 s two-broker
`docker pause`, ~9,390 outstanding at peak). **Both arms were re-measured on the
same master-based Rust build**, so the diagnosis-phase numbers in §4 are not
reused for the comparison.

| | before | after |
|---|---|---|
| Peak RSS, 10 KB payload | 449.4 MiB | **246.8 MiB** (-45%) |
| Peak RSS, 50 B payload | 327.9 MiB | **151.8 MiB** (-54%) |
| tracemalloc bytes/block at `producer.py:258` | 44,032 | **191** |
| `producer.py:258` total at peak | 379.9 MiB | **3.3 MiB** (-99%) |
| RSS after `malloc_trim(0)` vs baseline | 123.8 vs 24.5 MiB | **103.7 vs 24.4 MiB** |

Per-record marginal cost, least-squares over the rising phase only (the earlier
endpoint arithmetic conflated a fixed offset with the slope):

| | before | after |
|---|---|---|
| RSS per outstanding record, 10 KB | 41.17 KiB | **22.40 KiB** |
| tracemalloc per outstanding record, 10 KB | 53.40 KiB | **12.40 KiB** |
| RSS per outstanding record, 50 B | 31.00 KiB | **12.54 KiB** |
| tracemalloc per outstanding record, 50 B | 43.47 KiB | **2.50 KiB** |

Full soak, `HI=true`, 6 partitions, same hang: peak RSS **393 -> 209 MiB**.

## 11. Success criteria — one is not met

- **">=70% peak-RSS reduction at 10 KB": NOT MET. 45%** (47.7% of growth above
  baseline). Reported as measured rather than reframed.

  The bookkeeping the fix targets is gone — 99% of it, 53.40 -> 12.40 KiB traced
  per record. What remains is memory the fix does not touch: the 10 KiB payload
  itself, plus **~10 KiB per outstanding record of native RSS that tracemalloc
  does not see**. That residue is payload-size-independent (12.54 KiB/record RSS
  against 2.50 KiB traced at a 50 B payload), so it is not a payload copy — it
  is per-record allocation on the Rust side, invisible before because the 43 KiB
  `BatchNode` dominated. That is a new finding and a candidate for the next
  slice; it is not something Fix A could have addressed.

- **"bytes/block drops to approximately the payload size": exceeded.**
  44,032 -> 191 bytes/block. The site now holds only the node and its slot
  block; the payload is allocated by the caller and was never at this site.

- **"Zero change to delivery semantics": met.** Both soak arms
  `verdict=PASS`, `missed=0`. Unpaced throughput 241k rec/s before vs 243k after
  (two runs each, ~3% run-to-run spread), CPU 5.3-5.4 ms per 1k records in every
  run. Recovery after unpause clean in both. The growth path is genuinely
  exercised here: at full tilt nodes fill to the 1000-record threshold, so every
  node walks 8 -> 16 -> ... -> 1024.

Test suites, all on the merged verification branch: Python binding unit tests
**78 passed / 2 skipped**; soak unit tests **130 passed**; C tests
`test_mock_producer` **25**, `test_mock_consumer` **19**, `test_kafka_producer`
**11**, all passing. The C tests do not reference `BatchNode` — it lives only in
`_confluentkafka.c`, which is Python-only — but they do exercise the Rust FFI
batch-send path the fix calls into, unchanged.

Incidental: a Rust producer error, `Can't find batch created for topic id ...`,
appears once during recovery in three of seven runs, **both with and without the
fix and both with and without an allocator change**. Pre-existing, unrelated to
this change, worth a separate look.

---

# Part 3: allocator experiments (no code change)

Same harness, same 10 KB / 150 s hang, ~9,390 outstanding at peak. Rows 1-4 use
the **pre-fix** extension so each allocator is measured against the original
defect; rows 5-6 add Fix A.

| # | variant | baseline | peak RSS | after drain | after 60 s idle | after `malloc_trim` |
|---|---|---|---|---|---|---|
| 1 | glibc default, pre-fix | 24.5 | 449.4 | 449.4 | 449.4 | 123.8 |
| 2 | `MALLOC_MMAP_THRESHOLD_=32768`, pre-fix | 24.5 | 415.1 | 359.0 | 359.0 | 120.7 |
| 3 | `MALLOC_ARENA_MAX=2`, pre-fix | 24.4 | **570.4** | 570.4 | 570.4 | 89.6 |
| 4 | **jemalloc** `LD_PRELOAD`, pre-fix | 30.0 | 447.9 | **108.8** | **108.8** | n/a |
| 5 | **Fix A**, glibc | 24.4 | **246.8** | 241.2 | 241.2 | 103.7 |
| 6 | **Fix A + jemalloc** | 30.1 | **257.2** | **115.9** | **115.9** | n/a |

All figures MiB. Under jemalloc, glibc's `malloc_trim` returns 0 and frees
nothing, which confirms jemalloc is actually in charge.

**The coordinator's question — does fixing the allocation make the allocator
choice moot? No.** The two are orthogonal, and the table separates them cleanly:

- **Peak is live memory.** Fix A takes it 449.4 -> 246.8. No allocator moves it:
  jemalloc peaks at 447.9, essentially identical to glibc's 449.4.
- **Plateau is allocator retention.** jemalloc takes it 449.4 -> 108.8 with no
  forced trim. Fix A barely touches it: 241.2 under glibc, still **216.8 MiB
  above baseline**. Fix A removes the allocations but the *interleaved* churn of
  payloads and Python objects still fragments the arena.

So Fix A alone does not fix the graph the operator is looking at, and jemalloc
alone does not fix the memory the process actually needs.

**jemalloc decay timing:** ~10 s, matching the default `dirty_decay_ms=10000`.
Measured twice in one run — 447.9 MiB at t=162 s down to 186 MiB by t=172 s, and
223.6 MiB at t=192 s down to 110.0 MiB by t=200 s.

**`MALLOC_MMAP_THRESHOLD_=32768` disappoints, and the reason is instructive.**
In an isolated C program, 9,400 x 44,016-byte allocations are returned in full on
free by *default* glibc (405 MB -> 1.4 MB) with no env var at all. So the plateau
was never "44 KB sits below the mmap threshold". It is fragmentation from the
interleaved, multi-threaded pattern: freed holes stranded between live payload
and Python objects, which glibc can only trim from the top of a heap. Pushing
just the `BatchNode`s to `mmap` leaves the 10 KiB payload allocations — which are
below any sane threshold — still creating the holes.

**`MALLOC_ARENA_MAX=2` was the worst option measured**, raising peak RSS to
570.4 MiB, 27% above default glibc. Single run, not repeated, so treat the
magnitude as indicative — but there is no evidence it helps, and some that it
hurts.

**Throughput cost: none measurable for any variant.** Unpaced 10 KB producer,
60 s, ~245k rec/s: glibc 251,152; `MALLOC_MMAP_THRESHOLD_` 245,263;
`MALLOC_ARENA_MAX=2` 245,462; jemalloc 246,264 rec/s. CPU 5.2-5.3 ms per 1k
records throughout. Run-to-run spread is ~3%, so all four are indistinguishable.
The feared `mmap`/`munmap` pair per node does not show up.

## 12. The instrumentation risk, stated plainly

Adopting jemalloc **on the soak host** would make the graph look healthy while
the underlying waste remained. That is the specific failure mode to avoid: the
bindings ship to users on glibc, so users would keep the behaviour while our
instrument stopped showing it. Row 4 is exactly that trap — a beautiful 108.8
MiB plateau with the 44 KB-per-record defect fully intact.

This argues for fixing the allocation (done, row 5) and treating the allocator
as a **separate, deliberate, shipped-with-the-product** decision, not a soak-host
tweak. If jemalloc is adopted it should be adopted for the users who have the
problem, and the soak should keep at least one glibc arm so the instrument still
reflects what users run.

Recommendation, for review not for action: rows 5 and 6 are both defensible.
Nothing further applied.

---

# Review of 1c424eac ("soak: address PR review findings from Copilot,
# semaphore-agent-reader and Ankith")

Second-pass review of the eight fixes claimed in 1c424eac. Every claim below
was independently executed (test run, manual repro, `git check-ignore`,
source grep, doc cross-check) rather than read-and-trusted. Commands and
output are not reproduced here; ask if you want the transcript.

## Verified correct (no defect found)

1. **Exit-code fix** (`soakclient.py` `_shutdown_watchdog`, now exits
   `EXIT_CONSUMER_WEDGED`=4). `run.sh` special-cases only `ret == EXIT_FATAL`
   (`run.sh:424`); every other code, including 4, falls through to the
   restart/rapid-failure logic — confirmed by reading the branch directly.
   The new test
   (`test_shutdown_watchdog_hard_exits_with_consumer_wedged_not_fatal`) calls
   the real `_shutdown_watchdog` function with a never-set `exited` Event and
   monkeypatched `os._exit`, so it genuinely exercises the watchdog path, not
   just the constant's value — confirmed by reading the test body, this is
   not a trivial assertion.
2. **create-ec2.sh required-value redesign.** All four account-identifying
   values (subnet, security group, IAM profile, AMI) are gated by the same
   `missing=()` check and none has a residual default — confirmed by reading
   the full variable-declaration block, no `${SOAK_EC2_AMI_ID:-ami-...}` or
   similar leaked through. `--terminate` skips the gate (`[[ -z
   "$TERMINATE_ID" ]]` guards it). Flags parsed after the env-derived
   defaults in the `while` loop, so a flag always wins over
   `SOAK_EC2_*`/`create-ec2.env` — confirmed by reading parse order, and by
   the new `test_required_values_can_come_from_environment_variables` /
   `..._via_flags` tests, both of which pass.
3. **`.gitignore` claim**, ran directly:
   `git check-ignore -v bindings/python/soak/create-ec2.env` → matches
   `.gitignore:18:*.env` (ignored, exit 0); the same command against
   `create-ec2.env.example` → no match (exit 1, tracked). Exactly as the
   commit message states.
4. **Manifest JSON escaping.** Extracted the exact bash+`json.dump` block
   from the diff and ran it standalone with `MANIFEST_BUILD_LABEL` and
   `MANIFEST_GIT_BRANCH` containing `"` and `\` characters. Output parses as
   valid JSON via `json.load`, and round-trips the literal quote/backslash
   content correctly. The old string-interpolation approach would have
   produced unparseable JSON for the same input.
5. **`--label` validation.** `[[ ! "$LABEL" =~ ^[A-Za-z0-9_-]+$ ]]` runs
   before the required-value check and before any `aws` call. New
   parametrized tests cover 5 invalid labels (comma, space, semicolon,
   equals, braces) and 3 valid ones, all asserting `calls == []` on
   rejection (i.e., no `aws` invocation happened) via a stub `aws` on PATH.
6. **Log-rotation fix.** Reproduced both branches manually against the real
   `run.sh` (not the test's stub scenario) with a log file pre-seeded near
   the limit: a rapid failure (lifetime < `RAPID_FAILURE_SECONDS`) does NOT
   rotate the log even though it crosses the limit; a non-rapid exit (lived
   past `RAPID_FAILURE_SECONDS`) DOES rotate once the limit is crossed. Both
   match the claimed intent exactly — this is not merely "moved a line",
   the control flow was checked end-to-end. See gap noted below re: test
   coverage of the positive case.
7. **Tracemalloc/README rewrite, core claims.** `BatchNode` is allocated via
   `PyMem_RawMalloc` at `_confluentkafka.c:674` — confirmed directly in
   source, exactly the allocator the new text calls out. `PyMem_RawMalloc`
   is one of the three allocator domains tracemalloc hooks when active
   (raw/mem/obj), so "IS a tracemalloc-traced domain" is correct, not just
   plausible. The two headline numbers the README cites — `44,032` and
   `191` bytes/block — match `soak-rss-spike-explainer.md:112-113` exactly.
   The old table's claim ("climbing RSS + flat tracemalloc ⇒ native growth")
   is genuinely false for this codebase given (7); the new framing avoids
   restating that heuristic. Minor accuracy note below.
8. **`otel-config.yaml` log level** — comment and value both changed
   consistently (`debug` → `warn`), with a documented revert path. Nothing
   to check beyond reading the diff; no behavioral risk.
9. **build.sh 1000 msg/s sweep.** Grepped the whole `bindings/python/soak/`
   tree for `80 msg/s`: every remaining hit (soakclient.py:1422, run.sh:41,
   README.md ×4) refers to the **non-HI** default, which is legitimately 80
   (confirmed against `run.sh:166`, `SOAK_RATE="${SOAK_RATE:-80}"`). Only the
   HI-mode line in build.sh's post-build usage text was stale (HI default is
   1000, confirmed at `run.sh:161`), and only that line changed. Sweep claim
   holds.
10. **Both "verified false" verdicts, re-derived independently:**
    - Copilot's syntax-error claim at `soakclient.py:611`: `ast.parse()` on
      the current file succeeds cleanly; the string at that line is a
      properly `\"`-escaped literal inside a `.format()` call. Verdict:
      **correctly rejected.**
    - semaphore-agent-reader's `_on_delivery`/`_record_delivery` claim: read
      `_on_delivery` (`soakclient.py:1334-1356`) — the call to
      `self._record_delivery(...)` sits inside the same outer `try` whose
      `except Exception as ex:` clause does the error accounting. Any
      exception `_record_delivery` raises is caught there. Verdict:
      **correctly rejected.**
11. **Test counts.** Ran the actual suite (installed `pytest`/`psutil` into a
    scratch venv, bypassing the broken corporate index): **148 passed**
    against 1c424eac. Extracted the pre-commit tree via `git archive
    1c424eac~1` (not checkout — see process note below) and ran it in
    isolation: **131 passed**. Both figures match the commit message
    exactly.

## Fix-if-cheap (test-coverage gaps, not defects)

1. **No regression test pins the positive log-rotation path.** The new
   `test_a_rapid_failure_that_pushes_the_log_past_the_limit_is_not_rotated`
   only covers "must not rotate on rapid failure." I manually confirmed the
   "ran for a while → still rotates" branch works, but there is no automated
   test for it, at either commit. A future refactor that accidentally
   deletes the `maybe_rotate_log` call from the `else` branch (turning "never
   rotate on crash" into "never rotate at all" — exactly the regression #5
   in the review brief worried about) would pass the full suite today.
   Recommend adding a companion test mirroring the existing one but with
   `lifetime >= RAPID_FAILURE_SECONDS` and asserting `.prev.bz2` **does**
   appear.
2. **`test_required_values_can_come_from_a_local_env_file`** writes
   `create-ec2.env` directly into the real `bindings/python/soak/` source
   directory (not `tmp_path`), guarded by an existence check and cleaned up
   in `finally`. It works and is clean today, but is the one test in this
   diff that touches the working tree outside `tmp_path`; a hard interrupt
   mid-test (or parallel test execution) could leave a stray file behind, or
   collide with a developer's real local `create-ec2.env`. Not a defect —
   the guard means it fails loudly rather than clobbering silently — but
   worth a `monkeypatch`-based redirect of `SCRIPT_DIR` if this file grows
   more tests in this style.

## Notes (risk assessment, not a call to fix now)

1. **`EXIT_CONSUMER_WEDGED` collapses "transient" and "permanent" wedges into
   the same code**, and the commit's own reasoning is explicitly about the
   transient (broker-roll) case only. Trace the failure mode: the watchdog
   only fires after `shutdown_started` (i.e. after `run.sh` sends SIGTERM —
   which happens either at operator-requested termination or at the ~50 MB
   log-rotation boundary). By the time that fires, the process has almost
   always been running far longer than `RAPID_FAILURE_SECONDS` (default
   60s), so `lifetime >= RAPID_FAILURE_SECONDS` is true, and the exit lands
   in the "ran for a while" branch — `rapid_failures` resets to 0 and it
   restarts promptly, every time, no matter how many times in a row this
   happens. If the wedge is ever caused by a genuine, reproducible bug in
   the binding's `close()`/`flush()` path (not a transient broker
   condition), this design change means the soak will never trip
   `give_up()`'s crash-loop detection for it — it will silently restart
   every log-rotation cycle (i.e. every several hours), forever, with only
   a one-line "Shutdown watchdog expired, hard-exiting" in the log to show
   for it. Before this commit, the same event triggered `EXIT_FATAL` and
   `give_up()` unconditionally, which is wrong for the common (transient)
   case but does force a human to look. This is an inherent tradeoff of the
   fix, not a bug in it, and the operational stakes here (K2 roll
   resilience) plausibly justify it — but the commit message doesn't
   discuss the non-transient case, and no test or monitoring hook
   distinguishes "wedged once" from "wedges every rotation cycle." Given
   this project's stated principle that "only gaps are a hard failure" and
   rolling is cluster-side/observation-only, a repeatedly-restarting process
   isn't a correctness gap, but it does erode the signal value of
   `give_up()` for this one failure mode. Consider a follow-up: count
   consecutive `EXIT_CONSUMER_WEDGED` occurrences across restarts (persisted
   somewhere `run.sh` can see, e.g. a counter file) and escalate to
   `give_up()`-equivalent after N in a row, rather than relying on
   `RAPID_FAILURE_SECONDS` which this failure mode structurally evades.
2. **README's "tracked RSS almost 1:1" framing is a synthesis, not a
   quote.** The bytes-per-block figures (44,032 / 191) match
   `soak-rss-spike-explainer.md` exactly (verified above), and the doc does
   contain a near-1:1 data point (`design/current/soak-rss-spike-explainer.md:78`:
   "9,600 × 44 KB = 422 MB predicted / 425 MiB measured") — but that's a
   different measurement run than the one the README's sentence is attached
   to (the 8,132-block/341.4 MiB `BatchNode` figure at line 40, whose
   contemporaneous total RSS growth was larger, ~620 MiB peak-over-baseline
   in the original 790 MiB spike). The literal string "1:1" and "tracked...
   almost 1:1" don't appear in the doc. This isn't a factual error — the
   doc does support "tracemalloc's accounting for this leak tracked RSS
   growth closely" as a general characterization — but it's worth being
   precise that it's the reviewer's synthesis across two different
   measurement passages, not a lifted number. Low stakes: the load-bearing
   correctness claim (PyMem_RawMalloc is tracemalloc-traced, contradicting
   the old table) stands on its own regardless of this framing.

## Deviation verdicts (the two "verified false, no fix" claims)

| Claim | My verdict | Basis |
|---|---|---|
| Copilot: syntax error at `soakclient.py:611` | **Correctly rejected** | `ast.parse()` succeeds on current file; string is properly `\"`-escaped |
| semaphore-agent-reader: `_on_delivery` doesn't cover `_record_delivery`'s exceptions | **Correctly rejected** | `_record_delivery(...)` call is textually inside the outer `try` whose `except Exception` does the accounting |

Both of your spot-check verdicts hold. I found no case among the eight where
your spot-check was wrong.

## Process note (not a finding about the commit — a note on my own process)

While re-deriving the "148 vs 131" test-count baseline, I ran `git checkout
<rev> -- bindings/python/soak` followed by `git stash`/`git stash pop` to
restore state, and the pop applied an **unrelated, pre-existing stash**
(`stash@{0}`, named `perf-investigation-diagnostics-DO-NOT-DROP`, from a
different branch's work) onto this working tree, producing a merge conflict
and several stray modified/untracked files. I caught it immediately via
`git status`, ran `git reset --hard HEAD` and removed the two stray untracked
files it introduced (`COMMENTS.CRITIC1.md`, `src/common/debug_timing.rs`).
Verified afterward: working tree clean, HEAD unchanged, all three stash
entries (including the `DO-NOT-DROP` one) still present and untouched. I
switched to `git archive <rev> -- <path> | tar -x -C <scratch dir>` for the
rest of the baseline check, which touches no repo state. Flagging this only
so it's on record — no repo state was lost, but it's a reminder that this
shared repo currently has three stashes sitting on it from other
sessions/branches, at least one marked `DO-NOT-DROP`, and any agent reaching
for `git stash` here should assume it is not an empty/private stack.

## No rule/doc suggestions this round

Nothing here reveals a gap in CLAUDE.md, the admin/producer/consumer rule
files, or `agent-roles.md` — this commit is Python/shell tooling outside
their scope, and `review-expectations.md` / `python-soak-project.md`
memory already captures the right review posture for this area.
