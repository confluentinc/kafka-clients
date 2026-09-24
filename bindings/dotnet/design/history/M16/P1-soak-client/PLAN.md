# M16 / P1 — .NET soak-test client

**One phase. N=74.** A long-running producer/consumer endurance harness for the
Rust Kafka client, driven through the **.NET binding's public API** — the analog
of the Python soak merged upstream as PR #172 (`bindings/python/soak/`).

This is **not** a Kafka-logic translation. It is a client program that exercises
the already-shipped `Confluent.Kafka` .NET binding the same way the Python soak
exercises the Python binding. Ground truth for behaviour is the Python source in
PR #172; ground truth for the API it calls is `bindings/dotnet/src/Confluent.Kafka/`.

- **Mode A.** Zero Rust-core changes, zero ABI changes, zero new `[DllImport]`.
  Everything lands under `bindings/dotnet/soak/` plus one `Makefile` edit.
- **Branch:** `prashah_dev_dotnet_soak_test` — commit directly, no worktree.
- **Comment files:** `bindings/dotnet/COMMENTS.74.md` / `COMMENTS.DONE.74.md`.
- **Archive on close:** `bindings/dotnet/design/history/M16/P1-soak-client/`
  (the M15 `P<n>-<slug>` convention). "P1" is a directory-naming convention only;
  **there is no P2 planned**.

---

## 0 · Ground truth actually verified (not assumed)

Every claim below was checked against the tree on this branch. Where it
contradicts the task brief, the verified fact wins and the decision is restated
in §1.

| Claim | Verified | Where |
|---|---|---|
| `IAsyncProducer.Send(record, ct)` → `Task<RecordMetadata>`; second overload takes `IDeliveryCallback` | ✅ | `src/Confluent.Kafka/IAsyncProducer.cs:146,211` |
| `IAsyncConsumer.Poll(TimeSpan, ct)` → `Task<ConsumerRecords<K,V>>` | ✅ | `IAsyncConsumer.cs:95` |
| `IAsyncConsumer.Commit(IReadOnlyDictionary<TopicPartition,OffsetAndMetadata>, ct)` | ✅ | `IAsyncConsumer.cs:320` |
| `IConsumerCommon.Assignment()` → `IReadOnlyCollection<TopicPartition>`, sync | ✅ | `bindings/dotnet/CLAUDE.md §3` |
| `ProducerRecord` has **no** headers (`topic, value, key, partition, timestamp`) | ✅ | `ProducerRecord.cs:91,115-132` |
| `ConsumerRecord.SerializedValueSize` exists | ✅ | `ConsumerRecord.cs:150` |
| `OffsetAndMetadata(long offset, string? metadata = null, int? leaderEpoch = null)` | ✅ | `OffsetAndMetadata.cs:80` |
| `KafkaAdminClient(IReadOnlyDictionary<string,string> config)` — **same Java-shaped config** as producer/consumer | ✅ | `Admin/KafkaAdminClient.cs:46` |
| `NewTopic(string name, int? numPartitions, short? replicationFactor)` | ✅ | `Admin/NewTopic.cs:71` |
| `CreateTopicsResult.Values` → `IReadOnlyDictionary<string, Task>`; `All()` → `Task.WhenAll` | ✅ | `Admin/CreateTopicsResult.cs:91,98` |
| `KafkaException` carries `Code` / `IsRetriable` / `IsFatal` | ✅ | `KafkaException.cs:103-115` |
| `IDeserializer<T>.Deserialize(string topic, ReadOnlySpan<byte> data)` — sync, span | ✅ | `bindings/dotnet/CLAUDE.md §3` |
| **No** `OpenTelemetry*` reference anywhere in `bindings/dotnet/` | ✅ (control-positive: 7 `.csproj` match `Confluent.Kafka`) | `grep -rl OpenTelemetry --include=*.csproj --include=*.cs` → 0 |
| `Directory.Build.props` applies `TreatWarningsAsErrors`, `EnforceCodeStyleInBuild`, `Nullable=enable`, **strong-name signing** to EVERY project under `bindings/dotnet/` | ✅ | `Directory.Build.props` |
| `dotnet` resolves at `/usr/local/share/dotnet/dotnet` and `~/.dotnet/dotnet` | ✅ | filesystem |

### 0.1 ⚠ The brief's `PerformanceCommon` plan does not compile

The task brief proposes composing over `PerformanceCommon`'s `Bucket`,
`LatencyHistogram`, `PerfFormat`, `MemorySampler`, `CpuSampler` via a
`ProjectReference`. **Five of those seven types are `internal`:**

```
Bucket.cs:81            internal sealed class Bucket
LatencyHistogram.cs:25  internal static class LatencyHistogram
PerfFormat.cs:34        internal static class PerfFormat
MetricSamplers.cs:24    internal sealed class MemorySampler
MetricSamplers.cs:43    internal sealed class CpuSampler
```

Only `Metrics` (`public sealed`), `PerfEnv` and `PerfSignals` (`public static`)
are reachable across an assembly boundary — and `Metrics`' only constructor is
`public Metrics()` (`Metrics.cs:94`): **no path parameter, no append mode**,
which are precisely the two things the soak needs. So the compose plan fails on
both the accessibility and the capability axis. See D1.

---

## 1 · Decisions

Each is a ruling, not an option. Deviations from the task brief are marked ⚠ and
carry the reason.

### D1 ⚠ Fork the metric primitives into the soak project. Do NOT reference `PerformanceCommon`.

`bindings/dotnet/soak/SoakClient/` gets its **own** `Bucket`, `LatencyBucket`,
`PercentileFromHist`, `MemorySampler`, `CpuSampler`, `SoakEnv` (the `PerfEnv`
analog) and `SoakSignals` (the `PerfSignals` analog). **No `ProjectReference` to
anything under `tests/`.** The only project reference is
`../../src/Confluent.Kafka/Confluent.Kafka.csproj`.

**Why — two independent reasons, either sufficient:**

1. **Mechanical.** Five of the seven primitives are `internal` (§0.1). Reaching
   them needs an `InternalsVisibleTo` on `PerformanceCommon` — i.e. widening a
   *test-harness* assembly's surface to serve an operational tool, and with
   strong-naming in force that means embedding a public key too.
2. **This is what the Python original deliberately did, for a stated reason.**
   `soak_metrics.py`'s header is explicit:
   > PROVENANCE: copied from `bindings/python/test/performance/performance_common.py`
   > and **deliberately duplicated rather than imported**. […] a two-week run must
   > not break because a performance-test refactor changed a shared helper, and the
   > soak's own needs (append mode, a promptly-stoppable collector) must not
   > distort code the perf tests depend on.

   Both of the Python fork's named needs apply here verbatim: append mode
   (`run.sh` restarts the client and each restart must extend the series, not
   truncate it) and a promptly-stoppable collector (the soak's interval is 10 s;
   a `Thread.Sleep`-based sampler stalls every shutdown by up to 10 s).

**Obligation:** the forked file carries an equivalent `PROVENANCE` header naming
`tests/Performance/PerformanceCommon/` as the origin, and stating that **the
JSONL record schema must stay identical to the perf harness's** — that schema is
what makes soak and perf numbers comparable across clients. Diverging it is
allowed only with an explicit reason in the commit message.

**Consequence recorded:** the soak now duplicates ~200 lines of the perf harness.
That is the accepted cost, and it is the same cost the Python sibling accepted.

### D2 · Async facade (confirmed — the brief is right)

`AsyncKafkaProducer<byte[], SoakRecord>` + `AsyncKafkaConsumer<byte[], SoakRecord>`,
both loops as `async Task` joined with `Task.WhenAll`. Not one `Task` + one raw
`Thread`.

`IProducer.Send(record)` (sync) blocks until the broker acks — at the HI profile
(1000 msg/s) that serialises the whole send path. The Python soak's `send()`
returns a future and does not block on the ack, so the async facade is also the
faithful shape.

**Key type is `byte[]` with `Serdes.ByteArray`**, always `null` — Python's
`ProducerRecord(topic, value)` has no key. Note the producer serializer is
**invoke-on-null** by design (Java-faithful), and `Serdes.ByteArray.Serialize`
returns `null` for `null`, which is the absent-key sentinel. Correct as-is.

### D3 ⚠ Observe the returned `Task`. Prefer `ContinueWith` over `IDeliveryCallback`.

Python: `future.add_done_callback(lambda f, t=sent_at: self._on_delivery(f, t))`.
The literal .NET analog is `task.ContinueWith(...)`, and it is the recommended
shape because it (a) carries the per-send `sentAt` in the closure exactly as the
Python lambda does, and (b) **observes the `Task`'s exception**, which is
mandatory here.

**The mandatory part, whichever mechanism is chosen:** a fire-and-forget
`Task<RecordMetadata>` that faults and is never observed raises
`TaskScheduler.UnobservedTaskException` at GC. At 1000 msg/s with a broker roll
in progress that is a continuous stream of unobserved faults. Every send's
returned `Task` MUST have its exception observed — via the continuation that
implements `_on_delivery`, or an explicit `_ = t.Exception` on the faulted path.

`IDeliveryCallback` remains available and is not wrong, but it carries no
per-send state, so using it still requires a per-send allocation to hold
`sentAt` — no saving over the closure, and it does not observe the `Task`.

### D4 ⚠ The `SoakRecord` deserializer MUST NOT throw

This is the single highest-risk item in the phase.

`bindings/dotnet/CLAUDE.md §4` (Serializers): any `IDeserializer<T>` throw is
wrapped in a `SerializationException` and **faults the whole `Poll`**. Python's
`_consume_record` catches `ValueError` **per record**, counts
`consumer.msgerr`, and continues with the rest of the batch. A throwing
deserializer therefore converts "one corrupt payload" into "the entire fetch
batch is lost and the poll fails" — a behavioural divergence that would corrupt
the very accounting the soak exists to produce.

**Ruling:** `SoakRecordDeserializer` is **total**. On any malformed input it
returns a `SoakRecord` carrying a malformed marker (e.g.
`IsMalformed` + `MalformedReason`), never an exception. The consume loop checks
the marker, counts `consumer.msgerr`, and — exactly as Python does — **does not
let it drive the high-water-mark / duplicate / gap accounting** (a bad payload is
not a gap).

Also: when the value is **absent** (a tombstone), the binding returns
`default(TValue)` and **does not call the deserializer** (decision C). So
`record.Value == null` must be handled by the consume loop as malformed —
Python's `deserialize(None)` raises `ValueError("empty payload (None)")` and is
counted the same way.

**Rejected alternative:** deserialize the value as `byte[]` (`Serdes.ByteArray`)
and parse in the consume loop, which is the most literal Python port and sidesteps
the problem. Rejected because it reintroduces a per-record `byte[]` copy that the
span-based `IDeserializer` hook exists to avoid, and the total-deserializer shape
preserves Python's semantics exactly. Record the reasoning at the site.

### D5 ⚠ Match Python: poll `Assignment()`. Do NOT use `IConsumerRebalanceListener`.

The brief leaves this to the Actor. Ruling: **poll**, and record the listener as
a documented possible future improvement.

Python polled `assignment()` because its binding bridges no rebalance callbacks.
.NET *does* ship `IConsumerRebalanceListener` + `Subscribe(topics, listener, ct)`,
so the listener would be a genuine improvement — but not in this phase:

- The listener's three methods are **sync `void`** and fire on the **core's
  callback-dispatcher thread** (`CLAUDE.md §4` divergence D1/D3), with a
  no-throw obligation and a rebalance blocked until they return. That introduces
  a foreign-thread surface the Python original never had, inside a phase that is
  otherwise a straight port.
- `_check_assignment` doubles as a liveness probe on `Assignment()` itself; the
  listener does not replace that.

### D6 ⚠ Admin config: no JAAS translation layer at all

`KafkaAdminClient` takes the **same Java-shaped config** the producer and
consumer take. So Python's `jaas_field` / `jaas_credentials` /
`librdkafka_admin_config` (~85 lines plus their tests) are **deleted outright**,
not ported. `sasl.jaas.config` flows through untouched.

Topic creation becomes:
`admin.CreateTopics(new[] { new NewTopic(topic, partitions, repl <= 0 ? (short?)null : (short)repl) })`
then await `result.Values[topic]`, catching `KafkaException` and branching on
`.Code` for the same three-way split Python has: already-exists → log and
continue; auth/authz → `FatalStartupError` (exit 2); anything else →
`TransientStartupError` (exit 3).

**Recorded asymmetry, deliberately preserved:** Python routes the `admin.` prefix
but does **not** strict-validate the admin config against a key catalog (only
producer and consumer are validated). Keep that as-is. Do not invent an
`ADMIN_CONFIG_KEYS` catalog — mirror the original.

### D7 · Use `CancellationToken` for shutdown; drop the `_wakeup_sent` guard

`ffi-marshalling.md §B7`: a `CancellationToken` cancel maps to the consumer's
`wakeup()` internally, and surfaces as `OperationCanceledException` (not
`KafkaException(Wakeup)`).

So the .NET soak uses one `CancellationTokenSource` as the stop signal and does
**not** call `Wakeup()` explicitly. This deletes Python's `_wakeup_sent` Event —
which existed solely because each `wakeup()` arms the token again and shutdown
routinely delivers two signals. A `CancellationToken` is idempotent by
construction, so that hand-rolled guard has no job. Record the reason.

Consequently Python's `_is_wakeup(ex)` string-matching helper becomes
`catch (OperationCanceledException)` — a typed check replacing a message sniff.
Keep the behaviour it guarded: a commit aborted by shutdown is retried once
rather than counted as a failure.

### D8 · Metric identity: token `rust_dotnet`

`SOAK_CLIENT_TOKEN = "rust_dotnet"`, prefix `kafka.client.soak.rust_dotnet.`.
Same reasoning Python records for `rust_python`: Prometheus names must match
`[a-zA-Z_:][a-zA-Z0-9_:]*`, and the OTLP→Prometheus translation turns every other
character into `_`. One constant drives both the metric prefix and the host tag.

### D9 ⚠ Resource gauges: name them for what .NET actually measures

Python's gauges come from `resource.getrusage` + `psutil` + `tracemalloc`. The
.NET analogs, and one renaming:

| Python gauge | .NET source | Name |
|---|---|---|
| `cpu.user` / `cpu.system` | `Process.UserProcessorTime` / `PrivilegedProcessorTime` deltas | unchanged |
| `memory.rss` | `Process.WorkingSet64` | unchanged |
| `memory.rss.max` | `Process.PeakWorkingSet64` | unchanged |
| `memory.rss.baseline_imports` / `.baseline_constructed` / `.delta` | two RSS baselines, as Python | unchanged |
| `memory.tracemalloc` / `.peak` | `GC.GetTotalMemory(false)` + a running max | ⚠ **`memory.gc_heap` / `memory.gc_heap.peak`** |
| `producer.outq` | the soak's own `outstanding` counter | unchanged |

**Why the rename:** `GC.GetTotalMemory(false)` is the direct analog of
`tracemalloc` — managed-heap-only, so RSS climbing while it stays flat points at
native/Rust growth, which is the soak's headline question and the exact
diagnostic the Python RSS-spike investigation needed. But naming a .NET gauge
`tracemalloc` asserts a Python mechanism that is not running. Carry a comment at
the site stating it is the `memory.tracemalloc` analog, so a dashboard author can
map the two.

**.NET trap to get right:** `Process.GetCurrentProcess()` caches its values;
`.Refresh()` must be called before each sample or every window reports the same
number.

### D10 · `--check` mode uses the **mocks**, not a bogus broker

`Program.cs --check` must prove the assembly resolves and the native library
loads, then exit 0 / `EXIT_FATAL`(2). Do it by constructing and immediately
disposing an `AsyncMockProducer` / `MockConsumer` (plus parsing and validating the
config), **not** a real client against a fake broker address: the mocks go through
the same P/Invoke surface with no network, no DNS, and no timeout risk in a
preflight that `run.sh` blocks on.

### D11 · `Confluent.Kafka.sln`: SoakClient and its tests stay **OUT**

Follows the `grpc-server` / `PerfV2` / `PerfV3` precedent — operational tools with
extra dependencies are invoked by path, and `bindings/dotnet/CLAUDE.md §2`
describes the solution as the library plus its unit tests. Keeping SoakClient out
means `dotnet build $(SOLUTION)` does not restore the OpenTelemetry tree on the
functional gate's critical path.

**The consequence, stated so it is not lost:** `dotnet format $(SOLUTION)
--verify-no-changes` in `test-dotnet` therefore does **not** cover the soak
project. The new Makefile target MUST run `dotnet format` against the soak
projects by path, or the soak ships unformatted while the gate reads green.

### D12 ⚠ NEW EXTERNAL DEPENDENCY: OpenTelemetry NuGet packages

Verified: nothing under `bindings/dotnet/` references `OpenTelemetry` today
(control-positive: 7 `.csproj` match `Confluent.Kafka`). Adding
`OpenTelemetry` + `OpenTelemetry.Exporter.OpenTelemetryProtocol`
(+ `OpenTelemetry.Exporter.Console` if the `console` exporter is supported, as
Python's is) is a **new external dependency** under root `CLAUDE.md §1.2`.

It is in scope — the Python sibling already took the equivalent dependency
(`opentelemetry-distro`, `opentelemetry-exporter-otlp`) for the same reason: real
OTLP export from the soak box. **The commit message and the close-out MUST call
it out explicitly**, with the package ids and versions, so the maintainer sees it
as a dependency decision rather than as a file that quietly appeared.

Scope it to the soak project only. Versions must restore on net8.0 **and**
net10.0.

**Two Python branches that have no .NET analog — drop them, do not invent one:**

1. `_existing_real_provider()` — Python checks for a `MeterProvider` already
   installed by `opentelemetry-instrument` to avoid double-reporting. .NET has no
   such wrapper in play and builds its `MeterProvider` explicitly
   (`Sdk.CreateMeterProviderBuilder()`), owned by the soak. The branch drops;
   record that it dropped and why.
2. The grpc↔http exporter failover loop — Python fails over because *which*
   exporter package is installed varies. In .NET both protocols ship in one
   package selected by `OtlpExportProtocol`. Honour
   `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL`; no
   failover loop.

**The one behaviour that MUST port exactly:** if `OTEL_METRICS_EXPORTER` is unset
or `none`, log the reason and **return null** — never build a `Meter` with no
listener. A `Meter` with no registered `MeterProvider` silently discards every
measurement while the startup line claims "otel on". That was a real bug the
Python PR fixed after confirming it against a live collector (30+ minutes,
counters flat, no errors logged). Do not reintroduce its .NET twin.

Note the plain .NET OTel SDK does **not** read `OTEL_METRICS_EXPORTER` itself
(that is an auto-instrumentation variable) — the soak reads it, exactly as Python
does.

### D13 ⚠ The two bash-driving test suites are OUT of scope — deferred, not skipped silently

PR #172 ships `soak/test/test_run_sh.py` (403 lines) and
`soak/test/test_create_ec2_sh.py` (215 lines): pytest harnesses that execute the
bash scripts. They are **not** ported in this phase. Per
`definition-of-done.md §3` the reason is recorded rather than the omission being
silent:

- They test **bash**, and `create-ec2.sh` is copied verbatim while `run.sh` is
  ported with its supervisor logic (PID tracking, `EXIT_FATAL` never-restart, the
  rotation gate, backoff doubling) **unchanged** — that logic is already covered
  by the reviewed Python suite against the same script text.
- .NET's test tree has **no precedent** for driving bash from xunit; inventing
  that harness is a phase of its own and would roughly double this one, against
  the maintainer's explicit single-phase constraint.
- The genuinely *changed* parts of `run.sh` are thin: the `dotnet <dll>`
  invocation, env-var passing in place of the flag array, and the `--check`
  preflight.

**Candidate follow-up, maintainer-owned.** Flag it in the close-out.

---

## 2 · Deliverables

```
bindings/dotnet/soak/
├─ README.md                     # the runbook (operational tool on an EC2 box)
├─ ccloud.config.example         # ported; Java-dotted keys are already the shape
├─ create-ec2.sh                 # VERBATIM except the printed next-steps paths
├─ create-ec2.env.example        # verbatim
├─ otel-config.yaml              # verbatim
├─ bootstrap.sh                  # .NET SDK instead of python3-dev/pip/venv
├─ build.sh                      # dotnet build instead of venv+pip
├─ run.sh                        # supervisor: logic unchanged, invocation ported
├─ SoakClient/
│  ├─ SoakClient.csproj          # Exe, net8.0;net10.0; OUT of the .sln (D11)
│  ├─ Program.cs                 # env-driven entry, --check, signals, watchdog
│  ├─ SoakClient.cs              # the two loops + lifecycle + rusage
│  ├─ SoakRecord.cs              # payload type + total (de)serializer (D4)
│  ├─ HighWaterMarks.cs          # per-partition dup/gap accounting
│  ├─ SoakConfig.cs              # routing / validation / parsing
│  ├─ SoakMetrics.cs             # counters + gauges + JSONL (append)
│  ├─ MetricPrimitives.cs        # FORKED Bucket/LatencyBucket/percentile (D1)
│  ├─ ResourceSamplers.cs        # FORKED CPU/memory samplers (D1, D9)
│  ├─ SoakEnv.cs / SoakSignals.cs# FORKED PerfEnv / PerfSignals analogs (D1)
│  ├─ LastValueGauges.cs         # retain-and-re-yield last value per series
│  └─ OtelSink.cs                # OTLP, or null-with-a-reason (D12)
└─ SoakClient.Tests/
   └─ SoakClient.Tests.csproj    # xunit, net8.0;net10.0; OUT of the .sln
```

Plus one edit: `bindings/dotnet/Makefile` gains `test-soak-dotnet`, invoked by
`test-dotnet` — mirroring PR #172's `test: … $(MAKE) test-soak` wiring.

### 2.1 File-by-file notes

**`create-ec2.sh` / `create-ec2.env.example` / `otel-config.yaml`** — copy
verbatim. `create-ec2.sh` is language-agnostic AWS/EC2 provisioning; the only
edits are the printed next-steps text (`bindings/python/soak` →
`bindings/dotnet/soak`, and the bootstrap invocation). **Add no .NET-specific
logic to it.**

**`bootstrap.sh`** — same shape (validate the three `FILL_IN_*` placeholders →
apt packages → OTel Collector `.deb` → call `build.sh`). Drop
`python3-dev/pip/venv`; add the .NET SDK install covering the TFMs the soak
targets. **Keep `libjemalloc2` and the Rust toolchain** — cargo still builds the
native first, and the glibc strand the jemalloc option addresses is a property of
the *native* allocator, which the Rust core still uses.

**`build.sh`** — same `--ref` / `--src` source resolution, same
`cargo build --features ffi [--release]`, unchanged. Replace the venv/pip section
with `dotnet build -c $PROFILE` against the soak project **by path** (D11). No
venv and no `CONFLUENT_KAFKA_LIB_DIR`: `Confluent.Kafka.csproj` already computes
the native path from `target/$(CargoProfileDir)/` and fails loudly via its
`EnsureNativeLibraryExists` target if cargo has not run — verify that target
exists and rely on it rather than re-implementing the check.

Keep the **`build-manifest.json`** writer (git sha / describe / branch, toolchain
versions, build host and time, the `traceable` bool, the `--sha` override with
its loud not-traceable warning). Swap `python_version`/`venv` for
`dotnet --version` and the build output dir. **Keep the JSON-built-from-env-vars
pattern** — PR #172's own fix for a real injection bug, and the same risk exists
here because `--label` / `--sha` are operator-supplied.

Keep the post-build gate: `build.sh` runs the soak unit tests before handing over
an artifact (`pytest soak/test -q` → `dotnet test` by path).

**`run.sh`** — the supervisor loop is pure bash/OS logic and ports **unchanged**:
real child-PID tracking via `kill -0`/`wait` (never `ps | grep`, because four
soaks share a box), `EXIT_FATAL=2` → `give_up()` and never restart, the
`.FAILED` marker, the rotation gate that ensures a crash-restart within
`RAPID_FAILURE_SECONDS` never rotates away the crash evidence, backoff doubling
to a cap, `wc -c` rather than GNU `stat -c%s`, the HI-mode config append, the
`SOAK_JEMALLOC` `ldconfig` discovery with a hard failure when requested but
missing.

Three concrete edits:

1. Drop `SOAK_PYTHON` and the python-import preflight. Add `SOAK_DOTNET_ROOT` /
   the built `SoakClient.dll` path and invoke `dotnet "$SOAKCLIENT_DLL"`
   (framework-dependent; `bootstrap.sh` installs the runtime).
2. **Pass configuration as exported `SOAK_*` env vars, not a flag array.** Python
   built `-i/-t/-r/-f/...` for `argparse`; the .NET convention (`PerfV2`/`PerfV3`)
   is env-driven, so export `SOAK_TESTID`, `SOAK_TOPIC`,
   `SOAK_CONFIG_FILE="$EFFECTIVE_CONFIG"`, `SOAK_METRICS_FILE`, plus the
   already-computed `SOAK_RATE` / `SOAK_VARIANT` / `SOAK_PAYLOAD_SIZE` /
   `SOAK_PARTITIONS` / `SOAK_REPLICATION_FACTOR`. No CLI-parsing library.
   `SOAK_EXTRA_ARGS` becomes additional env assignments; document the change.
3. Preflight: replace the python import check with
   `dotnet "$SOAKCLIENT_DLL" --check` (D10).

**The exit-code contract is unchanged and is a hard contract** — `run.sh`'s
restart policy depends on it verbatim:

```
0 EXIT_OK               clean shutdown
1 EXIT_MESSAGE_LOSS     a gap was detected — the headline failure
2 EXIT_FATAL            config rejected, assembly/native missing, auth failed — NEVER restarted
3 EXIT_TRANSIENT_STARTUP broker unreachable at startup — worth retrying
4 EXIT_CONSUMER_WEDGED  poll failed past its bound, or shutdown wedged — restarted
```

`Program.cs` must return exactly these.

**`SoakRecord.cs`** — wire format `msgid|send_time_ms|txcnt|padding`, ASCII
decimal, identical to Python's. Still needed because `ProducerRecord` has no
headers (verified), so the metadata-in-payload trick ports as-is. Padding brings
the serialized record to the profile's target size (50 B normal / 10240 B HI).
Ships as an `ISerializer<SoakRecord>` / `IDeserializer<SoakRecord>` pair; the
deserializer is **total** (D4).

**`HighWaterMarks.cs`** — direct port. `offset <= hw` → `(hw+1) - offset`
duplicates; `offset > hw + 1` → `offset - (hw+1)` missed; the first offset on a
partition establishes the mark and is never counted. **The mark is set
unconditionally, not advance-only** — that is what makes a rebalance replay
report exactly the redelivery count once, with the replayed records then counted
in order. Carry the equivalent of
`test_hwmark_offset_zero_blind_spot_is_a_known_limitation`: the zero-offset blind
spot is inherited from the Java reference soak and must **not** be "fixed".

**`SoakConfig.cs`** — port `filter_config` (prefix drop + strip),
`route_shared_config` (a key unknown to this client but known to the other is
routed away, not rejected), `validate_config` (raise naming every unknown key,
with the same rationale: the Rust core only *warns* on an unknown key, so a typo
would silently start a multi-day run on the default value — e.g. an
unauthenticated PLAINTEXT connection), `stringify_config`, `parse_config_file`.
Keep the same `PRODUCER_CONFIG_KEYS` / `CONSUMER_CONFIG_KEYS` catalogs and the
`ssl.` accepted prefix — same Rust core, same accepted Java-dotted keys. Delete
the JAAS helpers (D6).

**`LastValueGauges.cs`** — direct port: retain and re-yield the last value per
**(metric, tag-set)**, so an event-driven gauge does not vanish from the backend
between updates. Retention is per tag-set, not per metric name
(`consumer.e2e_latency{partition=0}` and `{partition=1}` are distinct series).
Lock-guarded dictionary; snapshot under the lock, yield outside it.

**`SoakMetrics.cs`** — counters + gauges layered on the forked primitives. Same
`LATENCY_GAUGES` (`producer.latency`, `consumer.e2e_latency`,
`consumer.recovery_ms`) and `SECONDS_ON_EXPORT` (`producer.latency`,
`consumer.e2e_latency`) sets, with the reason carried: values are **recorded in
ms and exported in s**, and the conversion happens **on the export path only** —
a 1 ms-resolution histogram fed seconds sends every sample to bucket 0 and
reports p50/p90/p99/p999 as zero, silently destroying the JSONL, which is the
artifact a two-week run is analysed from. `consumer.recovery_ms` is deliberately
absent from `SECONDS_ON_EXPORT` (its name asserts ms and it has no reference-soak
counterpart). Counters carry `{total, delta}` against the last rollover. The
JSONL file is **appended**, not truncated, and flushed on every write so a
`kill -9` cannot lose collected samples.

**`SoakClient.cs`** — ctor order matters and is load-bearing: route and validate
**all** configs **before** anything with a side effect (a rejected key must not
leave a topic behind) → create the topic → construct both clients (before either
loop starts, so a failure cannot leave a producer running with no consumer) →
seed the zero-valued counters that must appear in the metrics even at zero →
take the RSS baseline → start the sampler → start both loops.

Producer loop: batch pacing `batch = max(1, rate/100)`, sleep off the batch's
remaining time budget on a cancellable wait (never a bare `Task.Delay` that
ignores shutdown). `txcnt` counts **send attempts** and the retry loop re-runs
only when `Send` itself throws a **retriable** error. On shutdown: `Flush()` then
a final status line.

Consumer loop: `await consumer.Poll(TimeSpan, ct)` — `TimeSpan` removes the
"seconds not milliseconds" footgun Python needed a comment for. Per record:
deserialize (total), count `consumer.msg`, record e2e latency from the payload's
send time, `observe_message(record.SerializedValueSize, latencyMs)`, run the HWM
accounting, stage the offset as `record.Offset + 1`. Commit on
`commit_interval`, and a best-effort final commit so a restart does not replay the
window. Stall detection + `consumer.recovery_ms` on recovery. Two-tier poll
failure escalation: `max_poll_failures` (default 20) for retriable,
`NON_RETRIABLE_POLL_FAILURE_LIMIT` (3) for non-retriable — deliberately >1,
because every *client-side* error reports `UnknownServerError`, which
`is_retriable()` excludes, so one non-retriable poll error is a routine timeout
during a broker roll. Error classification into coordinator-move / disconnect /
other by code set and message markers, exactly as Python.

**`Program.cs`** — reads `SOAK_*` via the forked `SoakEnv`; signals via the
forked `SoakSignals` (`Console.CancelKeyPress` + `AppDomain.ProcessExit`); the
`--check` mode of D10; and the shutdown watchdog: if teardown exceeds
`SOAK_SHUTDOWN_TIMEOUT` (default 60 s), hard-exit with code **4**
(`EXIT_CONSUMER_WEDGED`), not 2 — a shutdown wedged on backpressure during a
broker roll is transient, and `run.sh` only restarts non-fatal codes.
⚠ .NET note: `Environment.Exit` runs `ProcessExit` handlers and can itself block;
make sure the watchdog path does not depend on the same handler it might be
racing. Whatever mechanism is chosen, the process must exit with exactly 4.

Startup failure classification is the contract with `run.sh` and must be
preserved: config rejected / fatal startup → 2; broker unreachable → 3;
unclassified → 3 **with the stack trace printed** (so the supervisor retries a
few times under its own rapid-failure bound rather than stopping dead on a
flapping broker).

**`README.md`** — the runbook. Cover the same ground as PR #172's, adapted: what
the soak is, the two profiles, the env contract, the exit-code contract, the
telemetry setup, the EC2 flow (`create-ec2.sh` → `bootstrap.sh` → `run.sh`), the
memory-allocator section, and a **"differences from the Python soak"** list —
which is where D1, D4, D5, D6, D7, D9, D12 and D13 get their user-facing
statement. It may be shorter than the Python original where sections do not
apply (no venv, no JAAS translation).

---

## 3 · Tests

`bindings/dotnet/soak/SoakClient.Tests/` — xunit, `net8.0;net10.0`, no broker, no
Docker, no native calls needed for the logic suites.

1. **`SoakRecord`** — serialize/deserialize round-trip at both payload sizes;
   padding reaches the target size; a prefix longer than the target emits
   unpadded; and the malformed table: wrong field count, non-numeric field,
   empty/absent value. Every malformed case must return a **marked** record, not
   throw (D4) — assert that explicitly, since a throwing implementation would
   still pass a naive "it is rejected" test while breaking the batch at runtime.
2. **`HighWaterMarks`** — the same in-order / duplicate / gap table the Python
   suite has, the first-offset-establishes-the-mark rule, the rebalance
   jump-back case, and the offset-zero blind-spot pin.
3. **`SoakConfig`** — prefix routing and stripping, shared-key routing in both
   directions, unknown-key rejection **with the message asserted** (per
   `definition-of-done.md §3`, the message is part of the contract — it names the
   offending keys and the accepted set), the `ssl.` prefix acceptance, and
   `key=value` file parsing including the malformed-line error.
4. **`LastValueGauges`** — a value recorded once is re-yielded on the next
   snapshot; two tag-sets of one metric do not overwrite each other.
5. **Exit-code constants** — pin `0/1/2/3/4` against their names, so the contract
   with `run.sh` cannot drift silently.

Wire `test-soak-dotnet` into `bindings/dotnet/Makefile`'s `test-dotnet`, and make
it run `dotnet format` on the soak projects by path (D11).

---

## 4 · Definition of Done

All of `definition-of-done.md` applies, with these phase-specific readings:

- **§2 / §3 (completeness, tests)** — measured against the **Python soak**, which
  is the thing being ported, not against a Java class. Every skipped Python test
  needs a written reason; D13 is the one such deferral and it is recorded here.
- **§7 (no non-Java structs)** — does not apply: the whole phase is a client
  program, not an API translation. Nothing here is claiming to mirror a Java type.
- **§10 (hot-path allocation audit)** — **N/A**, and say so explicitly rather than
  skipping silently. The soak is a load generator, not a library hot path; a
  per-send closure is the deliberate design (D3).
- **§11 (consumer trait surface)** — N/A; the soak consumes the shipped surface,
  it does not define one.
- Build: `dotnet build -c Release` with **0 Error(s), 0 Warning(s)** on both TFMs
  (`TreatWarningsAsErrors` is inherited from `Directory.Build.props`, so this is
  enforced, not aspirational).
- `dotnet format --verify-no-changes` clean on the soak projects (D11).
- Zero `TODO` / `FIXME`.
- **Mode-A proof** with a control-positive: `git diff --name-only <base>..HEAD`
  empty over `src/`, `src/ffi/`, `target/include/confluent_kafka.h`,
  `cbindgen.toml`, `Cargo*`; `internal static extern` count unchanged.
- Apache 2.0 headers on every new file, copyright Confluent Inc. (matching the
  Python soak's headers).
- Shell scripts: `shellcheck`-clean where the Python originals were, with the same
  `# shellcheck disable=` annotations carried across where they still apply.

---

## 5 · Risks

| Risk | Mitigation |
|---|---|
| A throwing `IDeserializer` silently converts one bad record into a lost batch | D4 — total deserializer, with a test that asserts *no throw*, not merely rejection |
| Unobserved faulted `Task`s at 1000 msg/s | D3 — every returned `Task` observed |
| The forked primitives drift from the perf harness's JSONL schema | D1 — PROVENANCE header states the schema is shared; divergence needs a stated reason |
| OTel misconfiguration silently discards all measurements while logging "otel on" | D12 — null sink with a logged reason; never a listener-less `Meter` |
| `Process` counters cached, every window reporting identical numbers | D9 — `.Refresh()` before each sample |
| Soak project unformatted because it is outside the solution | D11 — Makefile target formats by path |
| Sandbox shell traps fabricating a false PASS | §6 of the Actor brief, verbatim |

---

## 6 · Out of scope

- Any Rust core or C ABI change (Mode A).
- The two bash-driving pytest suites (D13).
- `IConsumerRebalanceListener` (D5) — recorded as a candidate improvement.
- CI wiring beyond `bindings/dotnet/Makefile`'s `test-dotnet`. No Semaphore job,
  no root-Makefile target: the soak is run by hand on an EC2 box.
- A C ABI / Python soak change of any kind. PR #172 is upstream and untouched.
