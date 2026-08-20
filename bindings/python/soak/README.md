# Rust client soak test

A long-running (2+ week) producer/consumer soak for the Rust Kafka client,
driven through `bindings/python`. It produces and consumes continuously,
detects duplicates, gaps and latency drift, and survives a cluster being rolled
underneath it.

Modelled on `confluent-kafka-python/tests/soak/`. The *structure* of that
client is ported faithfully — `SoakRecord`, `SoakClient`, one producer thread
plus one consumer thread, `filter_config` prefix routing, per-partition
high-water-mark bookkeeping, `incr_counter` / `set_gauge`, `getrusage`
sampling, periodic status lines. The client call sites are not, because this
binding mirrors the **Java** API rather than librdkafka's. See
[Differences from the Python soak](#differences-from-the-python-soak).

## Layout

```
soakclient.py            SoakRecord, SoakClient, producer + consumer threads
ccloud.config.example    the one client config; SASL via sasl.jaas.config
requirements.txt         psutil, confluent-kafka, pytest, (optional) OTEL
create-ec2.sh            create the EC2 instance the soak runs on
bootstrap.sh             one-time host setup (toolchain, jemalloc, collector)
otel-config.yaml         OpenTelemetry Collector config (fill in FILL_IN_*)
build.sh                 build a pinned version into a venv
run.sh                   supervise one soak: restart it, bound its log
test/                    unit tests (pytest)
```

One config file and one flag, as in the reference soak — there are no profile
files. `HI=true` is the whole high-throughput variant, the way the reference's
`--perf` is.

## Quick start

```bash
# 1. Build the client and the bindings into a venv.
#    Either from a git ref:
./build.sh --ref <tag-or-sha>
#    or from a source tree that arrived by scp (the repo is not public yet).
#    --sha is what makes the build traceable: a copied tree has no .git.
./build.sh --src /path/to/confluent-kafka-rust --sha <commit> --label "batch-1"

# 2. Configure the cluster.
cp ccloud.config.example ccloud.config    # then fill in endpoint + API key

# 3. Run under the supervisor.
source <source-root>/venv-soak/bin/activate
TESTID=soak1 ./run.sh ccloud.config                # 80 msg/s, 50 B
HI=true TESTID=soak2 ./run.sh ccloud.config        # 1000 msg/s, 10240 B

# Any SOAK_* tunable set in the environment wins:
SOAK_RATE=200 HI=true TESTID=soak2 ./run.sh ccloud.config
```

`HI=true` is high-throughput mode: the rate goes 80 → 1000 msg/s **and** the
payload goes 50 B → 10240 B (~10 MB/s), plus the client tuning that makes 10 KB
records perform sensibly is appended to a copy of your config —
`consumer.fetch.max.bytes=52428800`,
`consumer.max.partition.fetch.bytes=10485760`, `producer.batch.size=1048576`,
`producer.compression.type=lz4`. Without those the consumer fetches a handful of
records per poll and the producer sends a batch per record.

**There is no rolling switch.** Rolling is a property of the *cluster* — an
external CronJob restarts its brokers — and the client does nothing differently
for it. A rolling soak is this same command with `bootstrap.servers` pointed at
the rolled cluster; set `SOAK_VARIANT=848-rolling` (or
`848-hi-throughput-rolling`) so its metrics are distinguishable from a
concurrent steady-state soak. Nothing needs to exist for that label.

**The environment wins.** `SOAK_RATE`, `SOAK_PAYLOAD_SIZE`, `SOAK_PARTITIONS`,
`SOAK_REPLICATION_FACTOR`, `SOAK_VARIANT` and `SOAK_EXTRA_ARGS` are all
`${VAR:-default}` in `run.sh`, so anything exported on the command line
overrides the default. (An earlier version kept these in sourced profile files
using bare assignments, so `source` silently clobbered the operator's value —
`SOAK_RATE=200` looked like it worked and produced at 80.)

Or run the client directly, without the supervisor:

```bash
python soakclient.py -i soak1 -t my-soak-topic -r 80 -f ccloud.config \
    --variant 848-normal --payload-size 50
```

`soakclient.py --help` lists every flag. The ones that matter most:
`--runtime-seconds` (0 = forever; useful for a smoke test), `--payload-size`
(replaces the Python soak's `--perf`), `--metrics-file`, `--recreate-topic`
(destructive; see [Topic handling](#topic-handling)).

## The variants

| invocation | rate | payload | bytes/s |
|---|---|---|---|
| `TESTID=x ./run.sh cfg` | 80 msg/s | ~50 B | ~4 KB/s |
| `HI=true TESTID=x ./run.sh cfg` | 1000 msg/s | ~10240 B | ~10 MB/s |

Either can point at a rolled or an unrolled cluster — that is a `bootstrap.servers`
choice, plus a `SOAK_VARIANT` label — so the four soaks the batch runs are these
two commands against two clusters.

**`HI=true` raises the rate *and* the payload** — 80 → 1000 msg/s and 50 B →
10 KB, ≈10 MB/s and roughly 2500x the bytes of the normal variant. This
deliberately diverges from the Python reference soak, whose `--perf` flag raised
only the payload (`-r 80` for every variant); here high throughput means more
records *and* bigger ones. Override either with `SOAK_RATE=` / `SOAK_PAYLOAD_SIZE=`.

The batched pacing (`batch = max(1, int(rate / 100))`, then sleep off the batch's
remaining time budget) means at 1000 msg/s the producer paces in batches of 10;
at the normal 80 msg/s the batch is 1 and the batching is inert.

**Rolling is cluster-side.** An external K8s CronJob rolls the brokers. The soak
client never rolls anything, never bounces its own consumer, and has no code
path that differs for a rolled cluster: it observes and quantifies — coordinator
moves, disconnects, recovery time, assignment changes — and proves zero loss
across each roll.

The config must set `group.protocol=consumer`. This is mandatory: the client
defaults to `classic` and **construction fails** for it, because only the
KIP-848 consumer protocol is implemented.

## Verification model

Per-partition high-water marks, ported from the Python soak's consumer loop:

* `offset <= hw` → **duplicate**, `(hw + 1) - offset` messages
* `offset > hw + 1` → **loss**, `offset - (hw + 1)` messages
* the first offset seen on a partition establishes the mark and is never
  counted (consumption starts at an arbitrary committed position)
* deserialization failures are counted separately (`consumer.msgerr`) and
  skipped **before** the mark is touched — a corrupt payload must never read as
  a gap

The mark is set to the observed offset unconditionally, not advance-only: when
a rebalance replays a partition from an older offset, the single jump back
accounts for the whole replay exactly once, and the replayed records that
follow are in order.

End-to-end latency comes from the `send_time_ms` field in the payload.

### Only gaps are a hard failure

The final `SUMMARY` line reports `verdict=FAIL (message loss)` if and only if
`missed > 0`. Duplicates are expected, for two independent reasons:

1. **No rebalance listener.** The binding bridges no
   `ConsumerRebalanceListener` callbacks (`consumer.py`'s module docstring
   states this is out of scope — the FFI bridges no callbacks into the
   embedding language), so offsets cannot be committed on partitions-revoked.
2. **`enable.idempotence` is inert.** The key is accepted by
   `ProducerConfig`, but there is no producer-ID/sequence machinery behind it
   in `src/producer/internals/sender.rs`, and transactions are not exposed
   through the bindings.

**No exactly-once claims.** A duplicate count is a data point, not a defect.

## Metrics

Prefix `kafka.client.soak.rust_python.`, tags `{host, testid, variant}`, with
`host = "rust_python-{hostname}-{topic}"` so four soaks on one box stay distinct.

The `rust_python` token identifies *the Rust client driven through its Python
bindings* — as distinct from a future native-Rust soak (`rust`) and from the
librdkafka one (`python`). It is deliberately **not** the literal `rust(python)`:
Prometheus metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*`, and the
OTLP→Prometheus translation rewrites every invalid character to `_`, so
`…soak.rust(python).producer.send` would arrive as
`kafka_client_soak_rust_python__producer_send` — with a **double** underscore
from the two parentheses, easy to mistype in a query and impossible to guess.
`rust_python` survives unchanged. It lives in one constant
(`SOAK_CLIENT_TOKEN`) used by both the metric prefix and the host tag.

The OpenTelemetry instrumentation scope is still `confluent.kafka.soak.rust` —
a separate open question with the telemetry owner, deliberately left alone here.

`incr_counter()` / `set_gauge()` are the only instrumentation entry points, as
in the Python soak. Behind them:

* **JSONL, always.** One JSON object per window (default 10 s) appended to
  `--metrics-file` — counters as `{total, delta}`, gauges as bucket rollups
  with `average/max/count` and, for the latency gauges, `p50/p90/p99/p999`.
  This is the durable local record a two-week run is analysed from, and it is
  what makes the soak fully runnable **before** the OTLP pipeline exists.
* **OpenTelemetry, additively**, when `OTEL_METRICS_EXPORTER` requests it.

### OpenTelemetry

The soak **configures the SDK itself** — a real `MeterProvider` with a
`PeriodicExportingMetricReader` and an OTLP exporter. It does not depend on being
launched under `opentelemetry-instrument`, because the supervised path
(`run.sh`) does not use a wrapper and a soak's telemetry must not hinge on how
the process was started.

```bash
OTEL_METRICS_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317 \
OTEL_METRIC_EXPORT_INTERVAL=60000 \
OTEL_SERVICE_NAME=kafka-client-soak-rust \
TESTID=soak1 ./run.sh ccloud.config
```

The standard variables are honoured — `OTEL_METRICS_EXPORTER` (`otlp`,
`console`, `none`, or a comma-separated list), `OTEL_EXPORTER_OTLP_*`
(endpoint, headers, protocol, certificate), `OTEL_METRIC_EXPORT_INTERVAL`,
`OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`. The instrumentation scope is
`confluent.kafka.soak.rust`.

**The startup log states the outcome, and it is never optimistic:**

```
telemetry: OTLP pipeline installed (exporters=otlp, interval=5000ms, endpoint=..., scope=...)
telemetry: reusing the MeterProvider already installed in this process — not installing a second one
telemetry: OTEL_METRICS_EXPORTER is unset or 'none'; metrics go to the JSONL file only
telemetry: DISABLED — could not build the otlp exporter pipeline: <reason>. Metrics go to the JSONL file only.
```

Three properties worth knowing, each of which was a real defect found against a
live collector:

* **A no-op meter is never used.** Calling `get_meter()` without installing a
  provider returns a meter that silently discards everything; the previous
  version did exactly that and still logged "otel on". If a pipeline cannot be
  built, the soak says `DISABLED` with the reason and falls back to JSONL.
* **An already-installed provider is reused, not replaced**, so running under
  `opentelemetry-instrument` does not double-report.
* **Event-driven gauges persist.** `consumer.assignment_size` and
  `consumer.recovery_ms` change rarely; a callback that yields buffered values
  and clears them makes such a series *disappear* from the backend after one
  collection rather than hold its last value. The last observation is retained
  per tag-set and re-yielded on every collection.

Telemetry can never take the soak down: the pipeline is built inside a
`try/except`, and the final flush is capped at 5 s so an unreachable collector
cannot stretch shutdown into the watchdog.

Counters: `producer.{send,drok,drerr,errorcb}`,
`consumer.{msg,msgdup,missedmsg,msgerr,error,errorcb}`, plus the net-new
`consumer.{rebalance,disconnect,coordinator_move}`.
Gauges: `producer.{latency,outq}`, `consumer.e2e_latency`, `cpu.{user,system}`,
`memory.{rss,rss.max}`, plus the net-new `memory.rss.delta`,
`consumer.{assignment_size,recovery_ms}`.

Notes on specific metrics:

* **`consumer.e2e_latency` is in SECONDS**, matching the Python soak (which
  reports `time.time() - txtime`), so dashboards read both clients on the same
  scale.
* **`producer.latency` is still in milliseconds.** The Python soak's is in
  seconds (`msg.latency()`), so this one does *not* match — mind the scale when
  comparing the two clients. Changing it is a one-line edit if wanted.
* The **JSONL percentiles and the log lines stay in milliseconds** for both. The
  histogram's buckets are 1 ms wide (`soak_metrics.MAX_LATENCY_MS`), so
  feeding it seconds would collapse every sample into bucket 0 and destroy the
  p50/p90/p99/p999 series.
* **`memory.rss.delta`** is RSS minus a baseline captured *after* client
  construction. A Python process's RSS includes CPython, its GC and the C
  extension, so "RSS climbed 40 MB in a week" is not by itself attributable to
  the Rust client; the delta is the separable signal. Two baselines are recorded
  once at startup — `memory.rss.baseline_imports` (after imports, before any
  client exists) and `memory.rss.baseline_constructed` — and their difference is
  what the client costs to construct.
* **`memory.tracemalloc` is how RSS growth gets attributed**, and is the reason
  `tracemalloc` runs by default. It measures the **Python-side heap only**, so
  read it against `memory.rss`:

  | `memory.rss` | `memory.tracemalloc` | reading |
  |---|---|---|
  | climbing | flat | growth is in the C extension or Rust |
  | climbing | climbing | growth is Python-side |
  | flat | flat | no leak |

  Without it, "RSS climbed 40 MB" is unattributable — which is the headline
  question the soak exists to answer. `memory.tracemalloc.peak` comes free from
  the same call. Frame depth is 1 (bookkeeping only, no traceback capture);
  `--no-tracemalloc` disables it if the overhead ever matters.
* **`producer.errorcb` / `consumer.errorcb` are always 0.** This client exposes
  no error callback. They are emitted so dashboards ported from the Python soak
  keep their series.
* **`broker.rtt.*` is absent.** Those came from librdkafka's `stats_cb`; the
  Rust client has no metrics layer (no `Sensor`, no `KafkaMetric`, no KIP-714).
  Tracked separately.
* **Rebalances are observed after the fact** by polling `assignment()` each
  loop, since no rebalance listener is bridged. `consumer.recovery_ms` is the
  length of a stall (`--stall-threshold`, default 5 s) that then recovered.
  5 s was previously a per-variant override for the rolled cluster; a 5 s
  stall is worth flagging anywhere, so it is now the uniform default.
* **`consumer.rebalance` is weaker than its name suggests — read
  `coordinator_move` / `disconnect` / `recovery_ms` for roll impact instead.**
  Two limitations, both inherent to inferring rebalances from assignment
  polling: (a) the initial `{} → {p0,p1}` transition counts, so **every process
  start contributes +1**, including every `run.sh` restart; (b) under KIP-848
  server-side assignment a broker roll typically moves the *group coordinator*
  without changing this member's partitions — it is the only member of its group
  — so a coordinator-only move is **invisible** here. The realistic steady-state
  value over a 14-day rolling soak is therefore exactly 1. Do not read a flat 1
  as "no rebalances occurred, detector healthy".
* **`producer.latency` includes any local backpressure wait.** The clock starts
  before `send()`, which blocks the calling thread when the accumulator is full,
  so this is wait + produce + ack — whereas the Python soak's `msg.latency()` is
  librdkafka's produce→ack only. At the normal 80 msg/s it never blocks; at 1000 msg/s × 10 KB (~10 MB/s)
  with `batch.size=1048576` a latency spike is ambiguous between "broker slow"
  and "we were blocked locally".
* **The metrics JSONL is not rotated, and the operator must size the disk for
  it.** Roughly 2.5 KB per 10 s window ≈ **22 MB/day/soak ≈ 315 MB over 14
  days**, so ≈**1.3 GB for the four variants** on one box. The log *is* bounded
  (50 MB + one `.prev.bz2`); the metrics file is deliberately not, because it is
  the analysis artifact and losing the early windows would defeat drift
  detection.
* **`disconnect` / `coordinator_move` are best-effort classifications** of the
  errors `poll()` / `commit()` raise, by protocol error code
  (`NotCoordinator`/`CoordinatorNotAvailable`/`CoordinatorLoadInProgress` vs
  `NotLeaderOrFollower`/`NetworkException`/...) with a message-substring
  fallback. Client-side errors all report `UnknownServerError` (-1), so the
  code alone is not enough.

### Creating the box, the collector, and one-time box setup

Three steps get from nothing to a running soak: **create the instance**
(`create-ec2.sh`), **deliver the source and bootstrap it** (`bootstrap.sh`),
then **run** (`run.sh`, above). The first two exist because nothing before
them creates or provisions anything — `bootstrap.sh` explicitly assumes the
box already exists.

* **`create-ec2.sh`** — launches the EC2 instance. Defaults to this project's
  existing, working configuration (region, AMI, instance type, subnet,
  security group, IAM instance profile, `cflt_*` governance tags) rather than
  generic guesses; override any of it with a flag if a second, independent
  host is ever needed (`--label` varies the name/tags so two can coexist).
  `--dry-run` performs `aws ec2 run-instances --dry-run` (an IAM permission
  check only) and creates nothing. `--terminate <id>` is the cleanup path — a
  forgotten running instance is a standing AWS bill, and there is no other one
  here. Creates its EC2 key pair automatically on first use if it does not
  already exist in the target region; AWS never returns key material again
  after creation, so losing that `.pem` means a new key, not a recovered one.
  This script does not create or modify a security group — the one in the
  defaults (or passed via `--security-group-id`) must already exist and be
  approved for this purpose.

The client pushes OTLP to a local collector; the collector is what reaches the
backend. Two files configure that side, mirroring the reference librdkafka soak
(`confluent-kafka-python/tests/soak/`):

* **`otel-config.yaml`** — the OpenTelemetry Collector config. It receives OTLP
  on `127.0.0.1:4317` and remote-writes to an Amazon Managed Prometheus (AMP)
  workspace over SigV4, assuming a cross-account writer role, plus a local
  `127.0.0.1:9464` Prometheus exporter for diagnostics. Fill in the three
  `FILL_IN_*` values (region, writer-role ARN, remote-write endpoint) before
  use — the account-specific ARN and workspace id are deliberately not committed
  (this repo is public); the live values live on the box and in the private
  handoff notes.
* **`bootstrap.sh <sha> [label]`** — one-time EC2 setup, run on the box
  `create-ec2.sh` just created. Installs the build toolchain, the Rust
  toolchain, `libjemalloc2` (for `SOAK_JEMALLOC`) and the OpenTelemetry
  Collector `0.130.0`, validates and installs `otel-config.yaml` to
  `/etc/otelcol-contrib/config.yaml`, restarts the service, then builds the
  client with `build.sh`. Rebuilds afterwards use `build.sh` directly.

The Rust repository cannot be cloned on the box (the Confluent GitHub org IP
allow list blocks it), so the source arrives by `git archive | scp` and
`bootstrap.sh` runs from the unpacked tree — which is why the commit SHA must be
passed explicitly (an scp'd tree has no `.git`). `create-ec2.sh` prints this
exact command, with the real IP and key path filled in, once the instance is
running.

## Observed behaviour during a broker outage

Measured by stopping the broker under a running soak for 123 s and restarting it
(single-broker local cluster, 80 msg/s, 50 B):

* **Zero loss, correctly.** `duplicates=0 missed=0 verdict=PASS`. 278 records
  failed delivery (their `delivery.timeout.ms` expired while the broker was
  gone) and were counted as `producer.drerr` — *not* as loss, because loss means
  a gap in the committed log, and a record the broker never accepted is not lost
  data.
* **`consumer.recovery_ms` worked**: reported 122891.9 ms.
* **`poll()` does not raise while the broker is down** — it returns empty
  batches. So a broker outage is handled by the stall/recovery path, not by the
  poll-failure bound, which is what a rolled cluster needs.
* **Stall detection is coarse during an outage.** The consumer thread parks
  inside `commit()` until its deadline (`default.api.timeout.ms`, ~60 s; the
  binding's `commit(offsets, timeout=...)` ignores the timeout argument), so the
  stall warning appeared at 64 s rather than at the 10 s threshold.
* **Producer memory grows while deliveries are stalled, and this is the one
  thing to watch.** Outstanding records accumulate (`producer.outq` reached
  6536) and peak RSS went from a ~41 MiB baseline to **212 MiB** over the 123 s
  outage — roughly 26 KB per outstanding record, which is per-record binding
  overhead rather than payload. A short broker roll is harmless; a *prolonged*
  outage on a box shared by four soaks is an OOM risk. The soak does not
  currently throttle producing when `producer.outq` grows, deliberately — adding
  an unreviewed backpressure mechanism was out of scope for the first batch.
  Watch `producer.outq` and `memory.rss`.

### Peak vs. plateau, and why `SOAK_JEMALLOC=true` exists

The 212 MiB above is the **peak** — live memory, genuinely in use while records
are outstanding. Separately, once the outage ends and everything is freed, the
memory does not reliably come back: glibc can only return freed pages to the OS
from the top of the heap, and this workload interleaves freed bookkeeping with
still-live payload and Python objects, stranding it. Measured on a repeated
stall: glibc RSS stayed at 449 MiB with no recovery at all over 60 s idle;
`malloc_trim(0)` then released 301 MiB on demand, proving it was never live —
just retained. Full investigation:
`design/current/soak-rss-spike-explainer.md`.

`SOAK_JEMALLOC=true` (see `run.sh --help`) addresses the **plateau**, not the
peak: jemalloc's decay-based purging returns that memory on its own, no forced
trim needed — measured 449 -> 109 MiB after the identical stall. It does not
reduce the peak; a separate fix in `_confluentkafka.c`
(`fix/python-binding-batchnode-memory`) does that, by sizing the per-record
bookkeeping to occupancy instead of a fixed 44 KiB.

Recommended for real batch runs, for exactly this reason. Keep at least one
variant running without it (the default) as a control — a healthy-looking
plateau can otherwise mask a future oversized-allocation regression the same
way it would have masked this one, before it was found.

## Configuration

`key=value` per line; everything after the first `=` is the value. Keys may be
prefixed `producer.` / `consumer.` / `admin.` to target one client; unprefixed
keys go to all of them, and an unprefixed key that only one client knows (e.g.
`group.id`) is routed to that one automatically.

Five traps, all encoded in the code rather than left to be rediscovered:

1. **`sasl.username` / `sasl.password` do not exist in this client.** The only
   credential path is `sasl.jaas.config`, in Java JAAS form:

   ```
   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="KEY" password="SECRET";
   ```

   Copying the Python soak's `ccloud.config` verbatim yields a mystery auth
   failure. `ccloud.config.example` here uses the JAAS form.

   The soak **refuses to start** if a SASL mechanism is configured but
   credentials cannot be recovered from the JAAS string, rather than silently
   connecting unauthenticated, and it logs the extracted **username** (never the
   secret) at startup so a config that parsed to the wrong principal is visible
   in the first lines of the log. An authentication failure during topic creation
   is reported as a one-line fatal error with exit code 2, which the supervisor
   treats as terminal.

   `ccloud.config` — and any `*.config` in this directory — is gitignored, since
   it holds a live API key.
2. **Unknown config keys are silently accepted by the client** — it only logs a
   warning and uses the default. A typo would therefore start a two-week run
   unauthenticated. So **the soak validates its configuration at startup and
   refuses to start**, naming the offending key. Validation runs before the
   topic is created, so a rejected config leaves nothing behind.
   The accepted-key lists in `soakclient.py` mirror
   `src/producer/producer_config.rs` and `src/consumer/consumer_config.rs`;
   keep them in sync.
3. **All config values must be `str`** — an int raises `TypeError` in the C
   extension. `stringify_config()` coerces.
4. **`consumer.poll(timeout)` takes seconds** (float), not milliseconds.
5. **`record.value` is a zero-copy `memoryview`** that dies with its batch.
   `SoakRecord.deserialize()` copies with `bytes(...)` before parsing.

## Payload format (headers are not supported on produce)

```
b"{msgid}|{send_time_ms}|{txcnt}|" + padding
```

The Python soak stamps `msgid` / `time` / `txcnt` as **record headers** and
reads the `time` header back for its end-to-end latency. This binding cannot:
`ProducerRecord` accepts only `{topic, value, key, partition, timestamp}`, and
`kafka_producer_ProducerRecord_t` has no headers field at all — all four FFI
send paths pass NULL headers. The Rust core *does* support headers
(`producer_record.rs::with_headers`), so this is purely an FFI/binding gap,
tracked separately; closing it is a change to the client, not to the soak.

So the soak carries the same three fields inside the value. `SoakRecord`
`serialize()` / `deserialize()` own the format and are the single source of
truth; padding brings each record up to `--payload-size`. Semantics are
identical to the Python soak's and no dashboard cares where the timestamp
physically sits.

`txcnt` counts *send attempts*. `send()` has no `BufferError` to retry — it
blocks the calling thread on backpressure — so it is >1 only when `send()`
itself raised a retriable error.

## Topic handling

The topic is **created if absent, never recreated**: `run.sh` restarts the
client repeatedly and a restart must not discard the soak's history.
`--recreate-topic` deletes and re-creates it, for a deliberate fresh start
only.

Topic creation goes through **librdkafka's** `AdminClient` (hence
`confluent-kafka` in `requirements.txt`) because this binding exposes no admin
API to Python. It is not on the produce or consume path — the soak drives the
Rust client for both. `librdkafka_admin_config()` translates the Java-style
config into librdkafka's namespace, unpacking the JAAS credentials into
`sasl.username`/`sasl.password` and dropping keys librdkafka would reject.

## Threading

Producer in one thread, consumer in another — the Python soak's topology.
Producer `send()` is thread-safe (the C extension takes a mutex under
`Py_BEGIN_ALLOW_THREADS`); the `Consumer` is single-owner, so exactly one
thread touches it. The **sync** `KafkaProducer` / `KafkaConsumer` are used, not
the asyncio flavors.

Two consequences:

* **Delivery callbacks fire on the C extension's poll thread**, so every shared
  counter is guarded by one `threading.Lock`.
* **The producer has no `wakeup()`**: a `send()` parked on backpressure,
  `flush()` and `close()` are all uninterruptible. The consumer *does* have
  `wakeup()`.

Shutdown: SIGINT/SIGTERM sets a `threading.Event` (which the producer's pacing
sleep waits on instead of sleeping) and calls `consumer.wakeup()`, then a
**watchdog thread hard-exits after `--shutdown-timeout` seconds** (default 60)
so a wedged `flush()`/`close()` cannot occupy a box shared by four soaks. The
handler uses `os.write(2)` rather than `print`, which can raise a reentrant
call.

`wakeup()` is issued **at most once per process**. Each call arms the token
again and so aborts one more blocking operation, and shutdown routinely
delivers two signals — a Ctrl-C reaching the whole process group, plus
`run.sh`'s own SIGTERM. (Java's `wakeup()` is a flag, and is idempotent while
one is pending.) A commit that a wakeup does abort — it lands between two polls
— is retried once rather than counted as a failure.

## `build.sh` / `run.sh`

Both are shell, a deliberate deviation from CLAUDE.md §6 ("xtask instead of
shell scripts"): what they orchestrate is a Python program in a virtualenv, an
xtask cannot bootstrap a venv it does not yet have, and the Python soak's
proven pair is what operators will recognise. (The earlier xtask proposal
targeted a *Rust* soak binary and no longer applies.)

**`build.sh`** resolves a source tree (`--ref` clones a git ref; `--src` builds
an scp'd directory — the repo is not public yet, so that mode is load-bearing;
**pass `--sha <commit>` with `--src`**, since a copied tree has no `.git` and the
manifest would otherwise record `"git_sha": "unknown"`, defeating its whole
purpose in exactly the mode this README recommends — `build.sh` warns loudly at
build time when that happens, and the soak repeats the warning at every startup
for the life of the run),
runs `cargo build --release --features ffi` (which produces
`target/include/confluent_kafka.h` and `target/release/libconfluent_kafka.*`),
creates a venv, installs `requirements.txt`, then installs the bindings with
`CONFLUENT_KAFKA_LIB_DIR` pointing at the cargo output — that order is
mandatory, since the C extension compiles against the generated header and
links the library. It then imports both modules as a smoke check and writes
`build-manifest.json` (git SHA, rustc version, build time), which
`soakclient.py` logs at startup so a two-week run is traceable to an exact
commit.

Submodules are deliberately not initialised: `kafka` is a multi-GB Java source
reference and `unity` only backs the C unit tests; neither is a prerequisite of
`cargo build --features ffi`.

**`run.sh`** supervises one child: restarts it if it exits, rotates the log
above 50 MB (bzip2, keeping one `.prev.bz2`), and stops it cleanly on
SIGINT/SIGTERM.

### Exit codes and the restart policy

The child's exit code is a contract, because the worst failure mode for an
unattended soak is a *permanently* broken child restarted every few seconds: the
process table looks healthy, and each restart bzip2s a 2 KB fragment over the
single `.prev.bz2` and deletes the log, destroying the evidence of why it died.

| code | meaning | supervisor |
|---|---|---|
| 0 | clean shutdown | restart |
| 1 | **message loss detected** | restart (the loss is in the log and metrics) |
| 2 | **fatal**: config rejected, bindings missing, authentication failed | **stop — never restart** |
| 3 | transient startup failure (broker unreachable) | restart |
| 4 | consumer wedged (`poll()` failed past its bound) | restart — it re-authenticates and re-joins |

On top of that:

* **A pre-flight runs before the supervise loop.** `run.sh` invokes `$PYTHON`
  once to `import soakclient`; if that fails it prints the interpreter's own
  error verbatim (the `ModuleNotFoundError` naming the missing package is the
  useful line), writes the `.FAILED` marker and exits 2 without starting
  anything. This exists because an import-time crash happens *before* `main()`,
  so the child cannot choose its exit code: the interpreter's exit 1 would
  arrive as "message loss" and be restarted with backoff, eventually leaving a
  marker that names the wrong problem. An incomplete venv on a fresh box is a
  likely first run.

  **The contract is still not airtight, and this does not claim otherwise:**
  exit 1 from the child can in principle mean an interpreter-level crash
  *after* `main()` has started (a `SystemExit`, an unhandled error in a path
  that bypasses `main()`'s handlers) rather than message loss. The pre-flight
  closes the common startup case, not the general one. Read the `SUMMARY` line:
  a genuine loss report always has one, with `missed=` non-zero.
* **Rotation only happens when the log is actually at the limit**, or when the
  supervisor itself stopped the child *for* rotation. A crash never rotates, so
  the evidence survives.
* **Consecutive rapid failures are bounded.** A child that lives less than
  `SOAK_RAPID_FAILURE_SECONDS` (60) counts as a rapid failure; the restart delay
  doubles from `SOAK_RESTART_DELAY` (5 s) up to `SOAK_RESTART_DELAY_MAX` (300 s),
  and after `SOAK_MAX_RAPID_FAILURES` (5) the supervisor **gives up**. A child
  that ran normally and then died resets both counters, so an isolated crash
  after three days restarts promptly.
* **Stopping is loud.** The log gets a banner, and a
  `<TESTID>-<VARIANT>.FAILED` file is written next to it carrying the reason,
  variant, topic and paths — so a dead soak is visible with `ls`, without
  reading 50 MB of log. Nothing is rotated or deleted in that state.

Two deliberate fixes versus the Python version:

* It **tracks the real child PID**. The Python version kills its child with
  `ps --ppid $PID -f | grep soakclient.py | xargs kill`, because its child is
  the tail of a `tee | bzip2` pipeline. With four soaks on one box that grep
  matches siblings.
* It does not use `stat -c%s` (GNU-only); the log size comes from `wc -c`.

Consequently the child writes straight to a plain log file rather than
streaming through `tee /dev/tty | bzip2`; compression happens at rotation, and
the live view is `tail -f` on the log. The metrics JSONL is opened in **append**
mode, so a restart adds to the series instead of truncating it.

## Tests

```bash
cd bindings/python && python -m pytest soak/test -v
```

Unit tests cover the pure logic no broker can verify: the `SoakRecord`
round-trip (including padding and every malformed-payload shape), the
duplicate/gap accounting table, config validation and routing, the JAAS
credential extraction, and the wakeup classification the commit path depends on.

**They run without the bindings installed**, which is what makes them runnable on
a Mac (see the note below). `soakclient.py` imports `producer` / `consumer`
lazily, through `_bindings()`, and nothing outside client construction references
those types: error codes and messages are read by duck typing
(`error_code` / `error_message` / `error_is_retriable`) rather than by
`isinstance(ex, KafkaError)`. `test_module_imports_without_bindings` asserts the
decoupling so it cannot silently regress.

`SoakClient.__init__` calls `_bindings()` before it creates the topic and before
any thread starts, so a missing or unbuilt binding fails at startup with an
actionable message and exit code 2 — never mid-run:

```
soakclient: startup error: the Rust client's Python bindings are not importable
(No module named '_confluentkafka'). Run bindings/python/soak/build.sh, or
activate the venv it created. ...
```

For an end-to-end smoke test against a local broker:

```bash
python soakclient.py -i smoke -t smoke-topic --replication-factor 1 \
    --runtime-seconds 60 -f local.config
```

Expect `duplicates=0 missed=0 ... verdict=PASS` in the `SUMMARY` line.

**The Python bindings only build on Linux** — `_confluentkafka.c` includes
`<threads.h>` (C11 threads), which macOS does not ship. So the soak itself only
*runs* on Linux, or in a Linux container. The unit tests above are deliberately
independent of that: they need only `pytest` and `psutil`, and pass on macOS with
no bindings present.
