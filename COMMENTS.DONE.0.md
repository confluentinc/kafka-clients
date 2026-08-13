# Critic 0 round 1 — resolutions

Review scope: `e7bcce7f`, `c9950b4d`, `5c5f9afa`, `a884593a`, `f86b9ec2`
(dev/python-soak-client). Twelve issues raised; five fixed as blockers for the
first batch, one test corrected, one cheap hardening applied, five deferred with
reasoning below.

The Critic's "verified correct — do not re-litigate" list is accepted as-is and
was not revisited.

---

## Fixed

### Issue 1 — `run.sh` crash-loops forever and destroys the evidence — FIXED
**Fixup of `5c5f9afa`.**

Confirmed exactly as described. The supervisor now keys its policy off the
child's exit code, which `soakclient.py` made into a documented contract:

| code | meaning | policy |
|---|---|---|
| 0 | clean | restart |
| 1 | message loss | restart (loss is recorded in log + metrics) |
| 2 | fatal: config rejected, bindings missing, auth failed | **stop, never restart** |
| 3 | transient startup failure | restart |
| 4 | consumer wedged | restart (it re-authenticates and re-joins) |

All three of the Critic's (a)/(b)/(c) are implemented:

* (a) exit 2 is terminal — `give_up()` logs a banner and exits.
* (b) consecutive *rapid* failures (child lifetime < `SOAK_RAPID_FAILURE_SECONDS`,
  60 s) are counted; the delay doubles from 5 s to `SOAK_RESTART_DELAY_MAX`
  (300 s); after `SOAK_MAX_RAPID_FAILURES` (5) the supervisor gives up. A child
  that ran normally then died resets both, so an isolated crash on day 3 restarts
  promptly.
* (c) `maybe_rotate_log()` gates rotation on the log actually reaching `LIMIT`, so
  a crash never bzip2s a fragment over `.prev.bz2`. An *intentional* stop for
  rotation is tracked separately (`stopped_for_rotation`) and is not counted as a
  failure — without that flag, a fast-filling log would have looked like a crash
  loop.

Terminal state is loud three ways: a banner in the log, a `.FAILED` marker file
next to it carrying the reason/variant/topic/paths (so `ls` shows a dead soak
without reading 50 MB), and run.sh's own exit code.

**Verified by execution**, not by inspection:

* Fatal config (exit 2): run.sh exited **2 after 1 s**, started the child
  **once**, left the log in place, and left a pre-seeded decoy `.prev.bz2`
  byte-identical (`PRECIOUS EVIDENCE FROM AN EARLIER RUN` still readable). Before
  this fix that scenario looped every ~6 s and overwrote `.prev.bz2` each time.
* Rapid-failure bound (fake child exiting 3): `Rapid failure 1/4 … 4/4` with
  delays **1 → 2 → 4 → 4** (cap honoured), then `SOAK STOPPED`, exit 3, marker
  written, total 11 s.
* Rotation is not miscounted: with `SOAK_LOG_LIMIT_BYTES=1500` and
  `SOAK_MAX_RAPID_FAILURES=2`, **4 rotations** occurred with no `Rapid failure`
  line and no marker — and the metrics JSONL accumulated across all 5 child
  lifetimes (10 windows), confirming append mode still holds.

### Issue 2 — `ccloud.config` not gitignored — FIXED
**Fixup of `c9950b4d`** (the commit that documented creating the file).

`.gitignore` now carries `/bindings/python/soak/*.config`, deliberately broader
than the one filename so `k1.config` / `k2.config` are covered too.
`ccloud.config.example` does not match `*.config` and stays tracked.

Verified with `git check-ignore -v` rather than assumed:

```
.gitignore:30:/bindings/python/soak/*.config   bindings/python/soak/ccloud.config
.gitignore:30:/bindings/python/soak/*.config   bindings/python/soak/k1.config
.gitignore:30:/bindings/python/soak/*.config   bindings/python/soak/k2.config
```
and `ccloud.config.example` reports no rule, `git ls-files` still lists it.

### Issue 3 — SASL/JAAS parser and its failure path — FIXED (code), verification still BLOCKED
**Fixup of `c9950b4d`.**

Code fixes:

* `jaas_field()` replaces the hand-rolled `find("username=")` + quote scanner
  with a regex that accepts every legal spacing/quoting variant:
  `username="k"`, `username = "k"`, `username='k'`, bare `username=k`, newline
  separated, reversed field order, and `serviceName="kafka"` present. A
  lookbehind stops `myusername=` from matching `username=`. Nine parametrised
  tests, including secrets containing `+`, `/` and `=`.
* **Silently proceeding with no credentials is now impossible.**
  `librdkafka_admin_config()` raises `ValueError` naming the missing field when a
  SASL mechanism is configured but credentials cannot be recovered. That is the
  outcome worth refusing outright: it is what turns a typo into an opaque
  broker-side error minutes later.
* The extracted **username is logged at startup** (never the secret), so a config
  that parsed to the wrong principal is visible in the first lines of the log.
* `create_topic()` classifies `KafkaException`: authentication/authorization codes
  → `FatalStartupError` (exit 2, terminal); everything else →
  `TransientStartupError` (exit 3, retried). `main()` catches both, plus a
  catch-all that keeps the traceback for genuinely unknown failures but still
  exits 3 so Issue 1's bound applies. No startup failure can reach the old
  "traceback → exit 1 → infinite restart" path.

Verified: unreachable broker (`nosuchhost.invalid:9092`) now produces
`soakclient: transient startup error: could not create or verify topic 't': Failed
while waiting for controller: Local: Timed out.` and **exit 3** — one line, no
traceback.

**Still not verified, and cannot be by me: no live SASL authentication has ever
happened.** No Confluent Cloud credentials are available in this environment. The
Critic's requested check — one `--runtime-seconds 60` run against K1 with the
real `ccloud.config`, confirming `verdict=PASS` — remains **outstanding and must
be run by the operator before the batch starts**. What changed is that its
failure mode is now a one-line fatal error and a stopped supervisor instead of a
traceback in a crash loop.

### Issue 4 — unbounded consumer poll retry — FIXED
**Fixup of `c9950b4d`.**

`_poll_failure_is_terminal()` bounds the storm: consecutive failures are counted,
reset on the first successful poll, and on reaching the bound the run logs
`fatal`, records `fatal_reason`, calls `abort()` — so `terminate()` still prints
the SUMMARY/verdict — and exits 4, which the supervisor restarts.

**One deliberate divergence from the Critic's "Expected", because the suggested
form would have introduced a worse bug.** The Critic asked to escalate when
`error_is_retriable(ex) is False`. But `Errors::is_retriable()`
(`src/common/protocol/errors.rs:393`) does not include `UnknownServerError`, and
**every client-side error — `Timeout`, `Wakeup`, `IllegalState` — reports
`UnknownServerError`** (`kafka_error.rs:494-500`). So a routine poll timeout
during a broker roll looks non-retriable, and escalating on the first one would
kill the soak precisely when it is meant to be proving it survives a roll. Two
tiers instead: `--max-poll-failures` (20, ~10 s) for retriable errors, and
`NON_RETRIABLE_POLL_FAILURE_LIMIT` (3, clamped to never exceed the configured
bound) for non-retriable ones. Still bounded, still escalates, but not on one
timeout. Six tests cover both tiers, including
`test_a_single_non_retriable_poll_failure_is_not_fatal`.

**Field-tested by stopping the broker under a running soak for 123 s.** Finding:
`poll()` does **not** raise while the broker is down — it returns empty batches —
so a broker outage is handled by the stall/recovery path, not by this bound,
which is the correct division. The bound covers the cases the Critic named
(auth revoked mid-run, `RuntimeError("Consumer is already closed")`), which are
genuine raises. On broker restart the soak recovered on its own and reported
`recovery_ms=122891.9`, `duplicates=0 missed=0 verdict=PASS`, exit 0.

The secondary detail (no commits during the storm) is now bounded to ~10 s.

### Issue 5 — `memory.tracemalloc` dropped — FIXED, deviation withdrawn
**Fixup of `c9950b4d`.** The Critic is right and my deviation was wrong.

`tracemalloc.start(1)` runs before the clients are constructed (frame depth 1 —
bookkeeping only, no traceback capture), and `get_rusage()` emits
`memory.tracemalloc` and `memory.tracemalloc.peak`. `--no-tracemalloc` disables
it if overhead ever matters; it is **on by default** because the plan presented it
as the mitigation, not an option.

Both baselines are now recorded, as the plan asked: `RSS_AFTER_IMPORTS_MIB` is
captured at module scope (genuinely after imports, before any client exists) and
the post-construction one as before. The startup line reads e.g.
`baseline RSS: 15.348 MiB after imports, 41.180 MiB after client construction
(client cost 25.832 MiB)`.

One correction to the Critic's "Expected" ("emit both baselines once at
startup"): emitting once makes every *later* window report `average=0`, because an
empty bucket averages to zero — indistinguishable from "the baseline is 0 MiB".
Both are therefore re-emitted every window as constants, which is also what lets
a dashboard compute `rss - baseline` in any window. Verified: windows 1..n each
carry `baseline_imports=15.35`, `baseline_constructed=41.22`.

Measured on a live run, the attribution now works: RSS 43.5 MiB vs
`memory.tracemalloc` 5.16 MiB — the Python heap is a small fraction, so RSS
growth would be attributable rather than ambiguous. That was the whole point.

Also restored the reference's initial `get_rusage()` call before the main loop
(`soakclient.py:954`), which I had dropped: without it the first metrics window
carried no memory or CPU gauges at all.

### Issue 10 (test half) — the blind-spot test no longer blesses the defect — FIXED
**Fixup of `c9950b4d`.**

The `hw > 0` sentinel is **kept** for fidelity to the reference
(`soakclient.py:324`), as the Critic allowed. What changed is the test: the case
`("repeat of offset 0 is not counted", …)` is gone, replaced by
`test_hwmark_offset_zero_blind_spot_is_a_known_limitation`, which asserts the same
values but is named and documented as recording a reference-inherited defect,
states the blast radius (~2 records, first run against a fresh topic only), and
says explicitly that it *should* fail if someone fixes the sentinel. The
behaviour fix itself is deferred — see below.

### Issue 12 — `build.sh` smoke check — FIXED
**Fixup of `a884593a`.**

The check now also `import soakclient` (which exercises psutil and the
`performance_common` `sys.path` insertion) and runs `pytest $SOAK_DIR/test -q`.
Given Issue 1, a syntax error or missing dependency previously surfaced as a
supervised restart loop instead of a build failure. The suite needs no broker and
runs in ~0.05 s.

---

## Deferred, with reasoning

### Issue 6 — `_OtelSink` is not thread-safe — DEFERRED, comment left at the site
Accepted as a real latent bug; all three consequences (duplicate instrument
registration, the callback's list swap racing appends, unbounded `_gauge_values`
if the exporter never collects) are correctly analysed.

Deferred because it is unreachable today — `_OtelSink` is only constructed when
`OTEL_METRICS_EXPORTER` names an exporter, and nothing has ever run that way — and
because the fix should be written against a working OTLP pipeline (task 6) rather
than blind. Fixing it now would mean shipping an untested change to a path the
first batch does not execute, in the same commit as the paths it does.

A prominent `.. warning::` block now sits on the class documenting all three
races, naming the four threads involved, giving the two-line fix (move the
`self._otel.*` calls inside `SoakMetrics`'s existing lock, or give the sink its
own), and pointing here. Whoever enables OTEL cannot miss it.

### Issue 7 — metrics JSONL unbounded — DEFERRED, documented
Numbers confirmed: ~2.5 KB per 10 s window ≈ 22 MB/day/soak ≈ 315 MB per soak
over 14 days ≈ 1.3 GB for four variants.

Deliberately **not** rotated: the metrics file is the analysis artifact, and
rotating it would discard exactly the early windows that multi-day drift
detection needs. The Critic offered "or size the box for it deliberately and say
so" — that is the choice taken, and the README now states the per-day and
per-14-day figures next to the log's own bound so the operator sizes the disk.
"Silence is not fine" is addressed; 1.3 GB is acceptable for a first batch.

### Issue 8 — `consumer.rebalance` measures almost nothing — DEFERRED, documented
Both limbs accepted: the initial `{} → {p0,p1}` transition contributes +1 per
process start, and a KIP-848 coordinator-only move leaves a sole-member group's
assignment unchanged, so the steady-state value over a 14-day rolling soak is
most likely exactly 1.

The code change (seed the first observed assignment without counting it; rename
to `assignment_changed`) is deferred per the coordinator's instruction — renaming
a metric after dashboards exist is worse than documenting it, and the +1 is
deterministic and therefore subtractable. The README now says plainly that every
process start contributes +1, that a coordinator-only move is invisible to
assignment polling, that the steady-state value is 1, and that
`coordinator_move` / `disconnect` / `recovery_ms` are the genuine roll signals —
with the explicit warning not to read a flat 1 as "detector healthy".

### Issue 9 — `close()` not retried on wakeup-abort — DEFERRED
Accepted, including the Critic's own assessment that it is low-likelihood: it
needs the signal to land in the sub-millisecond slice after a successful commit
while `pending` is empty, i.e. inside the <1% of wall time spent outside
`poll()`. `_wakeup_sent` already removes the double-signal case that made this
reachable at all in practice.

Deferred because the shutdown path is the one path every restart exercises, and
this batch already changes `_commit`'s retry, `request_stop`'s idempotency and
the exit-code contract there. Adding a third change to the same path the day
before a 14-day batch is a worse trade than a rare skipped group-leave whose
only cost is waiting out `session.timeout.ms` on one restart. The Critic's
simpler suggestion — have `request_stop()` record that a wakeup is outstanding
and let the shutdown path retry whichever single operation it aborted — is the
right shape for next round.

### Issue 10 (behaviour half) — `hw > 0` sentinel — DEFERRED
Kept for fidelity; the test now documents rather than blesses it (above).
Deferring the `None`/-1 sentinel because it is the one place where diverging from
the reference changes what the soak *counts*, and the batch's headline number is
the duplicate/gap count. Blast radius is ~2 records at the very start of the
first run. Worth doing next round with its own regression tests.

### Issue 11 — `producer.latency` conflates backpressure — DEFERRED, documented
Accepted as described, and the Critic's "not worth blocking on" is taken. Moving
`sent_at` after `send()` would silently change what a ported metric name means
mid-flight; emitting the wait separately adds a metric outside the agreed
contract. The README now states that `producer.latency` = backpressure wait +
produce + ack (vs the reference's produce→ack), and that a spike at ~800 KB/s is
ambiguous between "broker slow" and "blocked locally".

---

## New finding from this round's testing — producer memory during an outage

Not in the Critic's list; found while field-testing Issue 4 and recorded here
because it is the most operationally significant thing I learned.

Stopping the broker under a running soak for 123 s: outstanding records
accumulate unboundedly (`producer.outq` reached **6536**) and peak RSS went from
a ~41 MiB baseline to **212 MiB** — roughly 26 KB per outstanding record, which is
per-record binding overhead, not payload. A short broker roll is harmless; a
prolonged outage on a box shared by four soaks is an OOM risk in the very process
being watched for leaks.

Deliberately **not** fixed now: throttling production on `producer.outq` is a new
behaviour, unreviewed, on the send path, the day before the batch. Documented in
the README under "Observed behaviour during a broker outage" with the numbers and
the advice to watch `producer.outq` and `memory.rss`. Recommend triaging it for
next round.

Two smaller observations from the same run, both client-side rather than
soak-side:

* `commit()` parks the consumer thread until `default.api.timeout.ms` (~60 s)
  while the broker is down — the binding's `commit(offsets, timeout=...)` ignores
  its `timeout` argument — which is why the 10 s stall warning appeared at 64 s.
* One commit error surfaced a Rust type name to the operator:
  `std::collections::hash::map::HashMap<confluent_kafka::common::topic_partition::TopicPartition, …> was 0 ms past its expiration`.

---

## Verification performed this round

* **84 unit tests** (was 61; +23 for the JAAS variants, the credential refusal,
  the poll-failure tiers, the exit-code contract and the documented blind spot):
  pass on **macOS with no bindings installed** and in the **Linux container**.
  `pyflakes` and `pycodestyle --max-line-length=100` clean in both.
* **run.sh failure paths, by execution**: fatal exit 2 is terminal and preserves
  a decoy `.prev.bz2` byte-identical; the rapid-failure bound trips at N with
  1→2→4 backoff; 4 rotations do not trip it.
* **Real broker end-to-end**: `produced=2053 delivered=2053 consumed=2052
  duplicates=0 missed=0 errors=0 verdict=PASS`, both baselines and both
  tracemalloc gauges present in every window.
* **Broker-outage survival**: 123 s total outage, `recovery_ms=122891.9`, 278
  delivery failures counted as `producer.drerr` and correctly **not** as loss,
  `duplicates=0 missed=0 verdict=PASS`, exit 0.
* Not verified: live SASL against Confluent Cloud (no credentials — Issue 3),
  `_OtelSink` (never constructed), `--recreate-topic`, and anything requiring
  more than a few minutes of runtime.

---

## Suggested rule/doc updates — for the user to decide

Per `agent-roles.md` an Actor must not edit `CLAUDE.md` or the rules files, so
both of the Critic's proposals are recorded here unmodified for a human to accept
or reject. I agree with both, and this round is evidence for the first: Issues 1,
2, 4 and 7 are each an instance of a gap the current DoD does not cover, and
three of them were blockers.

1. **`.claude/rules/definition-of-done.md`** — add a clause for non-translation,
   long-running operational deliverables, e.g.: *"For long-running or supervised
   tooling, additionally: no unbounded retry loop without an escalation path;
   every file the tool grows (log, metrics, state) has a stated bound; a
   documented credential file is gitignored in the same commit that documents it;
   and every restart-on-failure path distinguishes transient from permanent
   failure."* I would add one more, learned from Issue 1's rotation gate: *"a
   supervisor must not destroy the evidence of the failure it is reacting to."*
2. **`CLAUDE.md` §6** (xtask over shell scripts) — add an explicit carve-out so a
   settled decision is not reopened: *"except where the script bootstraps the
   toolchain that would run the xtask, or must mirror an external reference
   implementation operators already know; record the rationale next to the
   script."* The rationale for `build.sh`/`run.sh` is in the soak README.

---

# Telemetry owner round — resolutions

Five findings from a 30+ minute run against a real collector. All five valid,
all five fixed in this round; no pushback. This is the empirical evidence
`_OtelSink` was deferred pending, so Issue 6's deferral is withdrawn.

**One thing I could not do as instructed:** the cited in-tree reference
`metrics_emitter.py` **does not exist anywhere I can reach** — not in this
repository (`find` + `grep -rl "MeterProvider"` across all `*.py` return
nothing), not in the scratchpad, not under `/Users/pratyush` to depth 6. The SDK
setup below is written against the standard OpenTelemetry Python API rather than
copied from that file. If it lives in another repo, point me at it and I will
reconcile.

### T1 + T2 — `_OtelSink` never configured the SDK, and `available()` lied — FIXED
**Fixup of `c9950b4d`.**

Confirmed exactly as reported: `get_meter()` with no `MeterProvider` installed
returns a **no-op meter**, so every `add()` and every `Observation` was
discarded — silently, with the startup line still claiming "otel on". A false
health signal is worse than no telemetry.

`_OtelSink.create()` now builds a real pipeline: `MeterProvider` +
`PeriodicExportingMetricReader` + an OTLP exporter, honouring
`OTEL_METRICS_EXPORTER` (otlp/console/none, comma-separated),
`OTEL_EXPORTER_OTLP_*` (endpoint, headers, protocol, certificate — read by the
exporter itself), `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES` via `Resource.create()`. Protocol selection tries the
requested one then fails over, since which exporter package is installed varies.

`available()` is gone. `create()` returns a sink **or None with the reason
logged**, and `otel_enabled` reflects that, so the startup line reports an
outcome rather than an intention. An already-installed SDK `MeterProvider` is
detected by `isinstance` against the SDK class (the API's own default is a
proxy/no-op that looks the same) and **reused**, so `opentelemetry-instrument`
does not double-report. Telemetry cannot take the soak down: construction is
wrapped, and the final flush is capped at 5 s — an unreachable collector
previously stretched shutdown to ~19 s, measured; now 24 s total for a 12 s run
instead of 31 s.

Because the SDK is configured in-process, **`run.sh` needs no wrapper** — which
was the operationally important half of this finding.

### T3 — thread safety — FIXED
**Fixup of `c9950b4d`.** Deferral rationale expired, as the coordinator says.

`_OtelSink` now holds its own lock around instrument creation (the check-then-set
that could register a duplicate the SDK drops, silently losing a series), and the
gauge store `LastValueGauges` has its own. The callback snapshots under that lock
and yields *outside* it, so a generator never holds a lock for as long as the SDK
takes to consume it. `SoakMetrics` still calls in from outside its own lock,
deliberately — no nested locks, no deadlock risk. The third consequence
(unbounded `_gauge_values` growth if the exporter never collects) is gone
structurally: the store keeps one entry per tag-set instead of appending.

### T4 — sparse gauges vanished from the backend — FIXED
**Fixup of `c9950b4d`.**

Root cause as reported: the callback yielded the buffer then cleared it, so the
next collection yielded nothing — and a series that yields nothing is not
"unchanged", it is **absent**. Invisible for continuously-updated gauges,
fatal for the event-driven ones, which is exactly `consumer.assignment_size` and
`consumer.recovery_ms` — the two the rolling profiles exist to produce.

`LastValueGauges` retains the most recent observation **per (metric, tag-set)**
and re-yields it on every collection. Per tag-set, not per metric, so
`consumer.e2e_latency{partition=0}` and `{partition=1}` do not overwrite each
other — verified in the export, both partitions present. Cardinality is bounded
(partitions, error codes, fixed base tags), the same argument as the JSONL
counters. Six unit tests including a five-thread concurrency test.

Noted for the record: this also *reduces* export volume. Previously every
buffered observation was yielded — ~4800 per partition per 60 s window for
`e2e_latency` — which last-value aggregation collapsed to one anyway.

### T5 — build manifest untraceable in `--src` mode — FIXED
**Fixup of `5c5f9afa`.**

`--sha` and `--label` added. When git metadata is unavailable and no `--sha` was
given, `build.sh` prints a boxed warning at build time and records
`"traceable": false` plus `"git_sha_source": "none"`; **the soak then repeats the
warning at every startup**, because the log is what someone reads in two weeks,
not the manifest file. A `--sha` that disagrees with a real git HEAD is recorded
with both values rather than silently overriding.

### Verification — against a real collector, observed not asserted

`otel/opentelemetry-collector:latest` on the test network, OTLP/gRPC receiver,
`debug` exporter at `verbosity: detailed`. (First attempt showed nothing: my
collector config set `service.telemetry.logs.level: warn`, which suppresses the
debug exporter's own output. Corrected to `info`.)

Single clean run, **`run.sh` with no wrapper**, 5 s export interval:

* **13 export batches received.** `producer.send` climbed monotonically
  384 → 763 → 1125 → … → 4343, and the final value **equals** the client's own
  `produced=4343` in the SUMMARY line.
* **`consumer.assignment_size` present in 12 of the 13 batches**, value 2
  throughout — the assignment changed once, at startup, and the series persisted
  for the rest of the run. It is absent only from the first batch, which was
  exported before the consumer's first assignment poll. This is the T4 fix
  demonstrated end to end.
* `consumer.recovery_ms` correctly absent (no stall occurred).
* `consumer.e2e_latency` carried both `partition: Str(0)` and `partition: Str(1)`
  series — per-tag-set retention.
* Resource attributes honoured: `service.name: Str(soak-verify)`,
  `deployment.environment: Str(soak-verify)`; scope
  `confluent.kafka.soak.rust`; base tags `host`/`testid`/`variant` on data points.
* `verdict=PASS`, `duplicates=0 missed=0 errors=0`.

Other paths exercised against the same collector:

| scenario | observed |
|---|---|
| provider pre-installed (simulates `opentelemetry-instrument`) | `reusing the MeterProvider already installed` — exported under `service.name=preinstalled-provider`, counters 357→724→1083→1446, single not doubled |
| `OTEL_METRICS_EXPORTER` unset | `metrics go to the JSONL file only`, startup line says `otel not exporting (JSONL only)` |
| `OTEL_METRICS_EXPORTER=bogus` | `WARNING telemetry: DISABLED — unsupported OTEL_METRICS_EXPORTER 'bogus'`, soak continued, `verdict=PASS` |
| collector unreachable | pipeline installed, soak unaffected, `verdict=PASS`, shutdown capped |
| `--src` without `--sha` | boxed build warning, `traceable: false`, and `BUILD IS NOT TRACEABLE TO A COMMIT` at soak startup |
| `--src --sha <sha> --label ...` | `git_sha` recorded, `git_sha_source: --sha`, `traceable: true` |

100 unit tests (was 84; +16 for gauge retention, thread safety, exporter parsing
and the disabled paths) pass on **macOS with no bindings and no OTel installed**
and in the **Linux container with both installed**; pyflakes, pycodestyle(100)
and shellcheck clean in both.

**Not verified:** live SASL against Confluent Cloud (still no credentials — the
operator must run one 60 s pass against K1 before the batch); Prometheus itself,
since I verified at the collector rather than through a Prometheus remote-write
path; and `--recreate-topic`.
