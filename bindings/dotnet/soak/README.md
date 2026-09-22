# Rust client soak test — .NET binding

A long-running producer/consumer endurance harness for the Rust Kafka client,
driven through the **.NET binding's public API**. It produces at a fixed rate,
consumes what it produced, and adjudicates one question: **were any messages
lost?**

It is the sibling of `bindings/python/soak/`, which drives the same Rust core
through the Python binding. The structure is ported faithfully — the payload
format, the per-partition duplicate/gap accounting, the prefix-routed
configuration, the counters and gauges, the JSONL metrics schema, the supervisor
and its exit-code contract are all the same — so the two soaks' numbers and
dashboards line up. The client call sites are not ported, because this binding
mirrors the **Java** API. Every intentional difference is listed under
[Differences from the Python soak](#differences-from-the-python-soak).

---

## Layout

```
bindings/dotnet/soak/
├─ README.md                  this file
├─ ccloud.config.example      client config template (copy to ccloud.config)
├─ create-ec2.sh              provisions the EC2 box (AWS CLI)
├─ create-ec2.env.example     the account-specific values create-ec2.sh needs
├─ otel-config.yaml           OpenTelemetry Collector config for the box
├─ bootstrap.sh               one-time host setup, then build.sh
├─ build.sh                   cargo build --features ffi, then dotnet build + tests
├─ run.sh                     the supervisor: keeps the client alive, rotates its log
├─ SoakClient/                the client itself (net8.0;net10.0, Exe)
└─ SoakClient.Tests/          its unit tests (xunit, no broker, no Docker)
```

Both projects are deliberately **outside** `Confluent.Kafka.sln`, following the
`grpc-server` / `PerfV2` / `PerfV3` precedent: an operational tool with extra
dependencies must not sit on the functional gate's critical path. They are built,
formatted and tested **by path** — `bindings/dotnet/Makefile`'s
`test-soak-dotnet`, which `test-dotnet` invokes.

---

## Quick start

```bash
# 1. Build the Rust core and the soak client.
#    Either from a git ref:
./build.sh --ref v1.2.3
#    or from a source tree that arrived by scp (the repo is not public yet).
#    --sha is what makes the build traceable: a copied tree has no .git.
./build.sh --src ~/confluent-kafka-rust --sha "$(git rev-parse HEAD)" --label soak1

# 2. Configure the cluster.
cp ccloud.config.example ccloud.config && $EDITOR ccloud.config

# 3. Run under the supervisor.
TESTID=soak1 ./run.sh ccloud.config              # 80 msg/s, 50 B payloads
HI=true TESTID=soak2 ./run.sh ccloud.config      # 1000 msg/s, 10 KB payloads

# Any SOAK_* tunable set in the environment wins:
SOAK_RATE=200 SOAK_VARIANT=848-rolling TESTID=soak3 ./run.sh ccloud.config
```

`./run.sh --help` documents every environment variable.

---

## The variants

There is no profile file and no "rolling" switch: rolling is a property of the
**cluster** (an external CronJob rolls it), so a rolling soak is the same client
pointed at the rolled cluster with `SOAK_VARIANT=848-rolling` to label its
metrics.

| Variant | Rate | Payload | Set by |
|---|---|---|---|
| `848-normal` | 80 msg/s | 50 B | the default |
| `848-hi-throughput` | 1000 msg/s | 10240 B (~10 MB/s) | `HI=true` |

`HI=true` additionally appends the four client settings a 10 KB payload needs
(`consumer.fetch.max.bytes`, `consumer.max.partition.fetch.bytes`,
`producer.batch.size`, `producer.compression.type=lz4`) to a **copy** of your
config. Without them the consumer fetches a handful of records per poll and the
producer sends a batch per record, so the variant would carry bigger records
without actually exercising throughput.

---

## Verification model

Every record's value carries its own metadata (see
[Payload format](#payload-format)), so the consumer can check three things per
partition with no broker-side help:

* **duplicates** — an offset at or below the partition's high-water mark;
  `(hw + 1) - offset` of them.
* **gaps** — an offset above `hw + 1`; `offset - (hw + 1)` messages missed.
* **end-to-end latency** — now, minus the send time stamped into the payload.

The first offset seen on a partition only *establishes* the mark and is never
counted: Java and librdkafka both start consuming at an arbitrary committed
position, which is not a gap. The mark is then set **unconditionally**, not
advance-only — so when a rebalance replays a partition from an older offset, the
single jump back reports exactly the number of records about to be redelivered,
and the replayed records that follow are in order and counted once.

### Only gaps are a hard failure

`enable.idempotence` is accepted by the Rust client but **inert** — there is no
producer-id/sequence machinery behind it — and the .NET surface exposes no
transactions. Duplicates are therefore *expected* under retry, and a rebalance
replays whatever was not committed. So the SUMMARY verdict is:

| Outcome | Verdict | Exit code |
|---|---|---|
| a gap was detected | `FAIL (message loss)` | 1 |
| a loop gave up (poll wedged) | `ABORTED (<reason>)` | 4 |
| anything else | `PASS` | 0 |

Duplicates, message errors and consumer errors are **counted and reported**, not
failed on.

---

## Metrics

Two sinks, and the first is always written:

1. **JSONL**, one object per ~10 s window, **appended** to
   `soak-metrics-<variant>-<testid>.jsonl` and flushed on every write (so a
   `kill -9`, or the supervisor rotating a log, cannot lose collected samples).
   Appended rather than truncated because `run.sh` restarts the client and each
   restart must extend the series. **This file is the durable local record a
   two-week run is analysed from.**
2. **OpenTelemetry**, when `OTEL_METRICS_EXPORTER` requests an exporter *and the
   pipeline could actually be built*. See below.

The JSONL record schema is deliberately **identical to the performance
harness's** (`bindings/dotnet/tests/Performance/PerformanceCommon/`) — the
`rss` / `cpu` / `latency` / `bytes` / `messages` blocks, the window bounds and
the `-inf` sentinel — so soak and perf numbers are directly comparable across
clients and languages. The soak adds four keys of its own: `prefix`, `tags`,
`counters` (each `{total, delta}` against the last window) and `gauges`.

Counters and gauges carry the prefix `kafka.client.soak.rust_dotnet.` and the
base tags `host` / `testid` / `variant`.

| Counter | Meaning |
|---|---|
| `producer.send` | records handed to the producer |
| `producer.drok` / `producer.drerr` | deliveries confirmed / failed |
| `producer.delivery.failure{err}` | failed deliveries by error code |
| `consumer.msg` | records consumed and parsed |
| `consumer.msgdup` / `consumer.missedmsg` | duplicates / **gaps** (the real counts) |
| `consumer.msgerr` | records whose payload could not be parsed |
| `consumer.error` | consumer operation failures |
| `consumer.rebalance` | observed assignment changes |
| `consumer.disconnect` / `consumer.coordinator_move` | broker-roll symptoms |

| Gauge | Meaning |
|---|---|
| `producer.latency{partition}` | send → delivery, ms recorded / **s exported** |
| `consumer.e2e_latency{partition}` | payload send time → consume, ms recorded / **s exported** |
| `consumer.recovery_ms` | how long a stall lasted, ms recorded **and exported** |
| `consumer.assignment_size` | partitions currently assigned |
| `producer.outq` | sends accepted but not yet delivered |
| `cpu.user` / `cpu.system` | process CPU since the last window, % |
| `memory.rss`, `.max`, `.baseline_imports`, `.baseline_constructed`, `.delta` | RSS and its two startup baselines, MiB |
| `memory.gc_heap`, `.peak` | managed heap only, MiB |

Two of those need their reason stated, because getting either wrong silently
destroys the data:

* **ms recorded, seconds exported.** `producer.latency` and
  `consumer.e2e_latency` are *recorded* in milliseconds and *exported* in
  seconds, and the conversion happens on the export path **only**. The latency
  buckets are 1 ms wide, so feeding one seconds sends every sample to bucket 0
  and reports p50/p90/p99/p999 as zero. `consumer.recovery_ms` is deliberately
  excluded — its name asserts milliseconds.
* **`memory.gc_heap` is the `tracemalloc` analog.** Read it *against*
  `memory.rss`: RSS climbing while the GC heap stays flat points at native/Rust
  growth, which is the soak's headline question and something RSS alone cannot
  answer.

### OpenTelemetry

The client configures the OpenTelemetry SDK itself, so the standard `OTEL_*`
variables work with no wrapper:

```bash
OTEL_METRICS_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
TESTID=soak1 ./run.sh ccloud.config
```

* `OTEL_METRICS_EXPORTER` — `otlp`, `console`, a comma-separated list, or
  `none`. **Unset or `none` disables telemetry and says so**; it never builds a
  meter nothing listens to. (That failure mode is real: the Python soak's
  predecessor logged "otel on" while silently discarding every measurement for
  30+ minutes against a live collector.)
* `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL` —
  `grpc` (default) or `http/protobuf`.
* `OTEL_METRIC_EXPORT_INTERVAL` — export period in ms (default 60000).
* `OTEL_SERVICE_NAME` / `OTEL_EXPORTER_OTLP_ENDPOINT` / `_HEADERS` /
  `_CERTIFICATE` are read by the SDK itself.

The startup log states exactly what happened — installed (with the exporters,
interval, endpoint and scope) or disabled with the reason. A telemetry failure
can never stop the soak: the sink is `null` and the JSONL file keeps being
written.

`otel-config.yaml` is the Collector config for the box: it receives OTLP on
`127.0.0.1:4317` and remote-writes to Amazon Managed Prometheus over SigV4.
Three `FILL_IN_*` values must be filled in before `bootstrap.sh` will run.

---

## The EC2 flow

```bash
# 1. From your machine: create the box (needs the AWS CLI and four account-
#    specific values -- see create-ec2.env.example).
./create-ec2.sh --label njc-rust-dotnet-soak-tests

# 2. Ship the source (the repo is not public and CANNOT be cloned on the box:
#    the org IP allow list blocks it -- auth succeeds, the IP check fails).
SHA=$(git rev-parse HEAD)
git archive "$SHA" | ssh -i ~/.ssh/NJC-KEY.pem ubuntu@<ip> \
    'mkdir -p ~/confluent-kafka-rust && tar -x -C ~/confluent-kafka-rust'

# 3. On the box: fill in otel-config.yaml, then bootstrap once.
cd ~/confluent-kafka-rust/bindings/dotnet/soak
$EDITOR otel-config.yaml
./bootstrap.sh "$SHA" njc-rust-dotnet-soak-tests

# 4. Run (one tmux pane per soak).
TESTID=soak1 ./run.sh ccloud.config
```

`bootstrap.sh` installs the build toolchain (Rust, the .NET SDK via
`dotnet-install.sh`, `libjemalloc2`), the OpenTelemetry Collector, and then calls
`build.sh`. Rebuilds afterwards are `build.sh` alone.

`create-ec2.sh` is copied from the Python soak essentially verbatim — it is
language-agnostic AWS provisioning — with only its printed next-steps paths
changed. Pass `--label` to run a second, independent host alongside the Python
soak's.

### Memory allocator

Under a producer stall (a broker that stops answering with its connections still
open — *not* a graceful restart), glibc can permanently strand freed memory:
interleaved payload allocations fragment the heap, and glibc can only return
memory from the top. jemalloc's decay-based purging returns it on its own.

This applies to the .NET soak exactly as it does to the Python one: **the strand
is a property of the native allocator, which the Rust core still uses here.** It
is not a Python artifact, and the .NET GC heap is a separate question (watch
`memory.gc_heap` against `memory.rss` for that).

`SOAK_JEMALLOC=true` preloads it. `run.sh` discovers the library through
`ldconfig` rather than guessing a path, and **refuses to start** if it was
requested but is missing — so a graph never looks like an unexplained regression
when the real cause is a missing package. The default stays **off** on purpose:
jemalloc's purging can mask a future oversized-allocation regression, so at least
one variant should run without it.

---

## Configuration

One `key=value` file. Everything after the **first** `=` is the value, so a JAAS
string containing `=` needs no escaping. `#` comments and blank lines are ignored.

Keys may be prefixed with `producer.`, `consumer.` or `admin.` to target one
client; unprefixed keys go to all of them, and an unprefixed key that only one
client knows (e.g. `group.id`, `linger.ms`) is **routed automatically** to that
one rather than rejected.

**The soak refuses to start on a key neither client recognises**, naming every
offending key and printing the accepted set. That is deliberate: the Rust client
only *warns* on an unknown configuration key
(`src/producer/producer_config.rs`, `src/consumer/consumer_config.rs`), so a typo
would otherwise start a multi-day run on the default value — e.g. an
unauthenticated PLAINTEXT connection. `run.sh` runs this validation in its
`--check` preflight, before anything is created.

Two settings are mandatory and the example file explains why:
`consumer.group.protocol=consumer` (the client implements KIP-848 only, and
construction *fails* for `classic`) and `consumer.enable.auto.commit=false` (the
soak commits explicitly so commit failures can be counted).

---

## Payload format

`ProducerRecord` carries only `{topic, value, key, partition, timestamp}` — the
underlying `kafka_producer_ProducerRecord_t` has no headers field — so the
metadata the reference soak puts in *record headers* goes into the value instead:

```
{msgid}|{send_time_ms}|{txcnt}|<padding>
```

ASCII decimal, byte-identical to the Python soak's. Padding brings the record up
to the profile's target size. `txcnt` counts **send attempts**, not retries.

The deserializer that parses it is **total — it never throws.** That is not an
optimisation: a throwing `IDeserializer<T>` is wrapped in a
`SerializationException` that faults the **whole `Poll`**, turning one corrupt
payload into a lost fetch batch. A malformed payload instead decodes to a marked
record, which the consume loop counts as `consumer.msgerr` and keeps **out** of
the high-water-mark accounting — a bad payload is not a gap.

---

## Threading

* One `Task` running the producer loop, one running the consumer loop, joined
  with `Task.WhenAll` at shutdown.
* Both drive the **async** facade (`AsyncKafkaProducer` / `AsyncKafkaConsumer`).
  The sync `Send` blocks until the broker acks, which at 1000 msg/s would
  serialise the whole send path; the Python soak's `send()` likewise returns a
  future without blocking on the ack.
* Delivery accounting runs in a continuation on each send's returned `Task`. That
  continuation is also what **observes** the `Task`'s exception — a
  fire-and-forget `Task<RecordMetadata>` that faults unobserved raises
  `TaskScheduler.UnobservedTaskException` at GC, and at 1000 msg/s with a broker
  roll in progress that is a continuous stream of them.
* One background thread rolls the metrics window over, waiting on an event rather
  than sleeping so shutdown is not stalled by up to a full window.
* Shutdown is **one `CancellationTokenSource`**. `Wakeup()` is never called
  explicitly: the binding maps a token cancel onto it internally and surfaces it
  as `OperationCanceledException`.
* A watchdog thread hard-exits with code 4 if teardown exceeds
  `SOAK_SHUTDOWN_TIMEOUT` (default 60 s) — `Flush` / `Close` are uninterruptible,
  and four soaks share one box, so a wedged shutdown must not need a human.

---

## Exit codes and the restart policy

**A hard contract**: `run.sh` keys its restart policy off these numbers verbatim,
and `SoakExitCodeTests` reads `run.sh` to confirm the two still agree.

| Code | Name | Meaning | `run.sh` |
|---|---|---|---|
| 0 | `Ok` | clean shutdown | restart |
| 1 | `MessageLoss` | a gap was detected — the headline failure | restart |
| 2 | `Fatal` | config rejected, native missing, auth failed | **never restarts**; writes `.FAILED` |
| 3 | `TransientStartup` | broker unreachable at startup | restart |
| 4 | `ConsumerWedged` | poll failed past its bound, or shutdown wedged | restart |

The supervisor additionally: tracks the **real child PID** (never
`ps | grep`, because four soaks share a box); gives up after
`SOAK_MAX_RAPID_FAILURES` restarts that each lived under
`SOAK_RAPID_FAILURE_SECONDS`, leaving everything on disk with a `.FAILED`
marker; doubles its restart delay only while failures keep being rapid; and
rotates the log **only** when it has actually reached `SOAK_LOG_LIMIT_BYTES` —
a crash-restart must never rotate, or it would bzip2 a few-KB crash fragment
over the single `.prev.bz2` and delete the evidence of why the child died.

---

## Tests

```bash
cd bindings/dotnet && make test-soak-dotnet      # format check + both TFMs
```

No broker, no Docker. The suites cover the parts a two-week run depends on being
right and that no broker can verify for us:

* **`SoakRecordTests`** — the payload round-trip at both sizes, the padding
  arithmetic, and the malformed table asserting **no throw** (a throwing
  implementation would pass a naive "it is rejected" test while breaking the
  batch at runtime).
* **`HighWaterMarksTests`** — the in-order / duplicate / gap table, the
  first-offset rule, the rebalance jump-back, and a pin on the offset-zero blind
  spot (see below).
* **`SoakConfigTests`** — prefix routing and stripping, shared-key routing in
  both directions, unknown-key rejection **with the message asserted**, the
  `ssl.` prefix, and `key=value` parsing including the malformed-line error.
* **`SoakOptionsTests`** — the `SOAK_*` environment contract and its defaults.
* **`SoakMetricsTests`** — the JSONL schema, counter `{total, delta}`, JSON
  escaping, append mode, and the ms-recorded / seconds-exported split.
* **`LastValueGaugesTests`** — retention per tag-set, and thread safety.
* **`OtelSinkTests`** — exporter/protocol parsing, and that an unrequested or
  unbuildable pipeline yields `null` rather than a silent no-op meter.
* **`SoakErrorClassificationTests`** — the two-tier poll-failure bound.
* **`SoakExitCodeTests`** — the exit-code constants, the watchdog's code, and
  `run.sh` agreement.

### The offset-zero blind spot is pinned, not fixed

`HighWaterMarksTests.OffsetZeroBlindSpotIsAKnownLimitation` documents a defect
inherited from the reference soak: "never seen" and "last seen at offset 0" are
the same state, so a duplicate of offset 0 — or a gap immediately after it — is
not counted. The blast radius is ~2 records, on the first run against a fresh
topic only. It is kept for fidelity with the Python and reference soaks. **If
someone changes the sentinel, that test should fail — that is the point of its
name.**

---

## Differences from the Python soak

Each of these is deliberate; the reason is what matters.

| # | Difference | Why |
|---|---|---|
| 1 | **The metric primitives are forked**, not shared with the perf harness. | Five of them are `internal` to `PerformanceCommon` and its `Metrics` has no path/append constructor; and a two-week run must not break because a perf-test refactor changed a shared helper. Same choice the Python sibling made, for the same stated reason. The **JSONL schema stays identical** — that is what makes the numbers comparable. |
| 2 | **The deserializer is total** (returns a marked record, never throws). | A throwing `IDeserializer<T>` faults the whole `Poll`; Python counts the bad record and continues. |
| 3 | **No JAAS translation layer.** | `KafkaAdminClient` takes the same Java-shaped config as the producer and consumer, so `sasl.jaas.config` flows through untouched. Python needed ~85 lines of translation because its topic creation goes through librdkafka's AdminClient. |
| 4 | **One `CancellationTokenSource`; no `_wakeup_sent` guard.** | A token is idempotent by construction. Python's guard existed only because each `wakeup()` re-arms the token and shutdown routinely delivers two signals. `_is_wakeup`'s message sniff becomes `catch (OperationCanceledException)`. |
| 5 | **`memory.tracemalloc*` → `memory.gc_heap*`.** | `GC.GetTotalMemory(false)` is the direct analog, but naming a .NET gauge `tracemalloc` would assert a Python mechanism that is not running. Map one onto the other in dashboards. |
| 6 | **`memory.rss.max` is a running max of sampled RSS**, not a kernel peak. | `Process.PeakWorkingSet64` **returns 0** on this platform (measured: .NET 10.0.302, macOS — `WorkingSet64` 38780928, `PeakWorkingSet64` 0), so the gauge would read a flat 0. The consequence is stated honestly: it is the peak of the soak's own per-window samples, so a spike entirely between two samples is invisible to it. |
| 7 | **Configuration is `SOAK_*` environment, not `argparse` flags.** | The .NET convention in this repo (`PerfV2`/`PerfV3`). `SOAK_EXTRA_ARGS` therefore does not exist — `run.sh` **fails loudly** if it is set rather than silently dropping your tuning. |
| 8 | **`stringify_config` is not ported.** | The parsed config is `IReadOnlyDictionary<string, string>` by construction; there is nothing to coerce. Python needed it because its C extension rejects non-string values. |
| 9 | **The payload's target size lives on the serializer**, not on class state. | .NET has an `ISerializer<T>` to hang it on, which also keeps the unit tests independent without Python's reset fixture. |
| 10 | **`SoakClient.CreateAsync`, not a constructor.** | Topic creation awaits, and a C# constructor cannot. The startup ordering contract is unchanged. |
| 11 | **No `--no-tracemalloc` switch.** | `GC.GetTotalMemory(false)` is a cheap read with no instrumentation to enable or disable, unlike `tracemalloc`, which must be started and costs a bookkeeping entry per allocation. |
| 12 | **The assignment is polled, not registered via `IConsumerRebalanceListener`.** | Matching the Python original. The listener exists in this binding and would be a genuine improvement, but its methods are sync `void`, fire on the core's callback-dispatcher thread with a no-throw obligation, and block the rebalance until they return — a foreign-thread surface the Python original never had. Polling also doubles as a liveness probe on `Assignment()` itself. **Recorded as a candidate follow-up.** |
| 13 | **No OTel provider-reuse check, and no grpc↔http failover.** | .NET has no `opentelemetry-instrument` wrapper installing a provider before `Main`, and both OTLP protocols ship in one package selected by an enum, so there is nothing to fail over to. |
| 14 | **`OpenTelemetry` 1.18.0 is a new external dependency.** | Scoped to `SoakClient.csproj` only; it never reaches `src/Confluent.Kafka`. The Python sibling took the equivalent dependency for the same reason. |
| 15 | **The two bash-driving pytest suites are NOT ported.** | See below. |

### Deferred: the bash-driving test suites

PR #172 ships `soak/test/test_run_sh.py` (403 lines) and
`soak/test/test_create_ec2_sh.py` (215 lines) — pytest harnesses that execute the
bash scripts. They are **not** ported in this phase, and the reason is recorded
rather than the omission being silent:

* They test **bash**. `create-ec2.sh` is copied essentially verbatim, and
  `run.sh`'s supervisor logic — PID tracking, the `EXIT_FATAL` never-restart
  rule, the rotation gate, the backoff doubling — is ported **unchanged**, so it
  is already covered by the reviewed Python suite against the same script text.
* .NET's test tree has **no precedent** for driving bash from xunit; inventing
  that harness is a phase of its own.
* The genuinely *changed* parts of `run.sh` are thin: the `dotnet <dll>`
  invocation, env-var passing in place of the flag array, and the `--check`
  preflight.

The one contract that spans both languages — the `EXIT_FATAL` number — **is**
tested here: `SoakExitCodeTests.RunShAgreesOnTheFatalExitCode` reads `run.sh` and
asserts it matches `SoakExitCodes.Fatal`.

**Candidate follow-up, maintainer-owned.**
