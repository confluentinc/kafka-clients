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
ccloud.config.example    key=value client config; SASL via sasl.jaas.config
requirements.txt         psutil, confluent-kafka, pytest, (optional) OTEL
profiles/                one .env per soak variant, sourced by run.sh
  848-normal.env                  80 msg/s, ~50 B, cluster not rolled
  848-rolling.env                 80 msg/s, ~50 B, cluster rolled externally
  848-hi-throughput-normal.env    80 msg/s, ~10 KB (~800 KB/s)
  848-hi-throughput-rolling.env   80 msg/s, ~10 KB, cluster rolled externally
build.sh                 build a pinned version into a venv
run.sh                   supervise one soak: restart it, bound its log
test/                    unit tests for the pure logic (pytest)
```

## Quick start

```bash
# 1. Build the client and the bindings into a venv.
#    Either from a git ref:
./build.sh --ref <tag-or-sha>
#    or from a source tree that arrived by scp (the repo is not public yet):
./build.sh --src /path/to/confluent-kafka-rust

# 2. Configure the cluster.
cp ccloud.config.example ccloud.config    # then fill in endpoint + API key

# 3. Run one variant under the supervisor.
source <source-root>/venv-soak/bin/activate
TESTID=soak1 ./run.sh profiles/848-normal.env ccloud.config
```

Or run the client directly, without the supervisor:

```bash
python soakclient.py -i soak1 -t my-soak-topic -r 80 -f ccloud.config \
    --variant 848-normal --payload-size 50
```

`soakclient.py --help` lists every flag. The ones that matter most:
`--runtime-seconds` (0 = forever; useful for a smoke test), `--payload-size`
(replaces the Python soak's `--perf`), `--metrics-file`, `--recreate-topic`
(destructive; see [Topic handling](#topic-handling)).

## The four profiles

| profile | rate | payload | bytes/s | cluster |
|---|---|---|---|---|
| `848-normal` | 80 msg/s | ~50 B | ~4 KB/s | not rolled |
| `848-rolling` | 80 msg/s | ~50 B | ~4 KB/s | rolled externally |
| `848-hi-throughput-normal` | 80 msg/s | ~10240 B | ~800 KB/s | not rolled |
| `848-hi-throughput-rolling` | 80 msg/s | ~10240 B | ~800 KB/s | rolled externally |

**The message rate is 80 for all four.** That is verified against the Python
soak, whose `run.sh` passes `-r 80` for every variant; its `--perf` flag only
sets a ~10 KB pad and switches to batched pacing. "High throughput" means
~200x the *bytes* at the same message rate, not a higher rate.

The batched pacing is ported anyway (`batch = max(1, int(rate / 100))`, then
sleep off the batch's remaining time budget) so raising `-r` later works. At 80
msg/s the batch is 1 and the batching is inert.

**Rolling is cluster-side.** An external K8s CronJob rolls the brokers. The
soak client never rolls anything and never bounces its own consumer: it
observes and quantifies — rebalances, coordinator moves, disconnects,
assignment changes, recovery time — and proves zero loss across each roll.

Every profile sets `group.protocol=consumer`. This is mandatory: the client
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

Prefix `kafka.client.soak.rust.`, tags `{host, testid, variant}`, with
`host = "rust-{hostname}-{topic}"` so four soaks on one box stay distinct.

`incr_counter()` / `set_gauge()` are the only instrumentation entry points, as
in the Python soak. Behind them:

* **JSONL, always.** One JSON object per window (default 10 s) appended to
  `--metrics-file` — counters as `{total, delta}`, gauges as bucket rollups
  with `average/max/count` and, for the latency gauges, `p50/p90/p99/p999`.
  This is the durable local record a two-week run is analysed from, and it is
  what makes the soak fully runnable **before** the OTLP pipeline exists.
* **OpenTelemetry, additively**, when `opentelemetry` is importable *and*
  `OTEL_METRICS_EXPORTER` names an exporter. Never a hard import; the soak runs
  unchanged without it. Adopting whatever the telemetry pipeline standardises
  on touches only `_OtelSink`.

Counters: `producer.{send,drok,drerr,errorcb}`,
`consumer.{msg,msgdup,missedmsg,msgerr,error,errorcb}`, plus the net-new
`consumer.{rebalance,disconnect,coordinator_move}`.
Gauges: `producer.{latency,outq}`, `consumer.e2e_latency`, `cpu.{user,system}`,
`memory.{rss,rss.max}`, plus the net-new `memory.rss.delta`,
`consumer.{assignment_size,recovery_ms}`.

Notes on specific metrics:

* **`producer.latency` / `consumer.e2e_latency` are in milliseconds** (the
  Python soak's are in seconds). Milliseconds match the 1 ms-resolution
  histogram that produces the percentiles.
* **`memory.rss.delta`** is RSS minus a baseline captured *after* client
  construction. A Python process's RSS includes CPython, its GC and the C
  extension, so "RSS climbed 40 MB in a week" is not by itself attributable to
  the Rust client; the delta is the separable signal.
* **`producer.errorcb` / `consumer.errorcb` are always 0.** This client exposes
  no error callback. They are emitted so dashboards ported from the Python soak
  keep their series.
* **`broker.rtt.*` is absent.** Those came from librdkafka's `stats_cb`; the
  Rust client has no metrics layer (no `Sensor`, no `KafkaMetric`, no KIP-714).
  Tracked separately.
* **Rebalances are observed after the fact** by polling `assignment()` each
  loop, since no rebalance listener is bridged. `consumer.recovery_ms` is the
  length of a stall (`--stall-threshold`, default 10 s) that then recovered.
* **`disconnect` / `coordinator_move` are best-effort classifications** of the
  errors `poll()` / `commit()` raise, by protocol error code
  (`NotCoordinator`/`CoordinatorNotAvailable`/`CoordinatorLoadInProgress` vs
  `NotLeaderOrFollower`/`NetworkException`/...) with a message-substring
  fallback. Client-side errors all report `UnknownServerError` (-1), so the
  code alone is not enough.

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

## `build.sh` / `run.sh`

Both are shell, a deliberate deviation from CLAUDE.md §6 ("xtask instead of
shell scripts"): what they orchestrate is a Python program in a virtualenv, an
xtask cannot bootstrap a venv it does not yet have, and the Python soak's
proven pair is what operators will recognise. (The earlier xtask proposal
targeted a *Rust* soak binary and no longer applies.)

**`build.sh`** resolves a source tree (`--ref` clones a git ref; `--src` builds
an scp'd directory — the repo is not public yet, so that mode is load-bearing),
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
SIGINT/SIGTERM. Two deliberate fixes versus the Python version:

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
credential extraction, and the wakeup classification the final commit depends
on.

For an end-to-end smoke test against a local broker:

```bash
python soakclient.py -i smoke -t smoke-topic --replication-factor 1 \
    --runtime-seconds 60 -f local.config
```

Expect `duplicates=0 missed=0 ... verdict=PASS` in the `SUMMARY` line.

**The Python bindings only build on Linux** — `_confluentkafka.c` includes
`<threads.h>` (C11 threads), which macOS does not ship. Develop and run the
soak on Linux, or in a Linux container.
