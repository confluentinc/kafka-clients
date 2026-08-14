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
