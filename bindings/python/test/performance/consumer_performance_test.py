#!/usr/bin/env python3
# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Consumer end-to-end latency benchmark.

Methodology (matches the C benchmark):
  * e2e latency per record = ``now_ms - record_timestamp_ms`` (wall clock,
    ``int(time.time()*1000)``), read right after touching the record bytes.
  * Consumer config: ``group.protocol=consumer`` (KIP-848),
    ``auto.offset.reset=latest``, ``fetch.min.bytes`` / ``max.partition.fetch.bytes``
    = 4 MiB, ``check.crcs=false`` (v2 only — the Rust client does not surface it).
  * Settle to the live edge (poll until empty) before load starts.
  * Time-based ``WARMUP_SECONDS`` (excluded from stats) then
    ``TEST_DURATION_SECONDS`` measured; per-``INTERVAL_SECONDS`` snapshots to
    ``metrics.jsonl`` (shared ``performance_common.Metrics`` schema, same as the
    producer perf test and the Rust harness).
  * Summary: min/avg + p50/p90/p95/p99/p999, throughput msg/s and MiB/s.

Backends (``CLIENT_VERSION``): ``3`` = the Rust binding (``consumer.py``
``KafkaConsumer``), ``2`` = ``confluent_kafka.Consumer`` (librdkafka baseline).

Load generation (like ``consumer-perf/compare/librdkafka_e2e.py``): if
``KAFKA_BIN`` is set, spawn ``kafka-producer-perf-test.sh`` and kill it at the
end; otherwise run consume-only (an external producer must feed the topic — the
in-suite pytest entry produces inside the broker container).

Run modes:
  * Manual / full:  ``python consumer_performance_test.py`` (env-driven), via the
    ``make consumer-perf-test-python`` target. Exits non-zero if ``P99_LIMIT_MS``
    is set and the measured p99 exceeds it.
  * In-suite / short: ``pytest`` collects ``test_consumer_e2e_latency`` which
    spins a Kafka broker via testcontainers, produces a short burst inside the
    container, runs this script consume-only, and asserts.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_BINDINGS = os.path.dirname(os.path.dirname(_HERE))  # bindings/python (consumer.py, _confluentkafka)
for _p in (_HERE, _BINDINGS):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from performance_common import Metrics, MAX_LATENCY_MS, percentile_from_hist, recreate_topic  # noqa: E402


def _now_ms():
    return int(time.time() * 1000)


def _env_int(name, default):
    v = os.getenv(name)
    return int(v) if v is not None and v != "" else default


def sasl_config_from_env(v2=False):
    """SASL config from the environment, matching producer_performance_test.py
    and the Rust perf test (tests/integration/producer_perf_test.rs): enabled
    only when SECURITY_PROTOCOL is SASL_PLAINTEXT or SASL_SSL and mechanism +
    username + password are all set. The v3/Java form uses sasl.jaas.config; the
    v2/librdkafka form uses sasl.username/sasl.password. Returns {} (a no-op)
    for PLAINTEXT/SSL or incomplete credentials."""
    security_protocol = os.environ.get("SECURITY_PROTOCOL")
    mechanism = os.environ.get("SASL_MECHANISM")
    username = os.environ.get("SASL_USERNAME")
    password = os.environ.get("SASL_PASSWORD")
    if security_protocol not in ("SASL_PLAINTEXT", "SASL_SSL") \
            or not all((mechanism, username, password)):
        return {}
    if not v2:
        sasl_jaas_config = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
            f"username=\"{username}\" \n\tpassword=\"{password}\";")
        return {
            "security.protocol": security_protocol,
            "sasl.mechanism": mechanism,
            "sasl.jaas.config": sasl_jaas_config,
        }
    return {
        "security.protocol": security_protocol,
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }


class Config:
    """Benchmark configuration, parsed from the environment."""

    def __init__(self):
        self.bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("TOPIC_NAME", "test-topic")
        self.client_version = os.getenv("CLIENT_VERSION", "3")  # 3=rust, 2=confluent-kafka
        self.group_id = os.getenv(
            "GROUP_ID", f"benchmark-{self.client_version}-{int(time.time())}")
        self.warmup_seconds = _env_int("WARMUP_SECONDS", 120)
        self.test_duration_seconds = _env_int("TEST_DURATION_SECONDS", 600)
        self.interval_seconds = _env_int("INTERVAL_SECONDS", 1)
        self.poll_timeout_ms = _env_int("POLL_TIMEOUT_MS", 1000)
        self.message_size = _env_int("VALUE_SIZE", 2048)
        self.throughput = _env_int("THROUGHPUT", 125000)  # producer msg/s (KAFKA_BIN path)
        self.num_messages = _env_int("NUM_MESSAGES", 0)  # 0 => duration-based
        self.partitions = _env_int("PARTITIONS", -1)  # -1 => broker default
        # When True (default), delete + re-create the topic before consuming
        # (see performance_common.recreate_topic).
        self.create_topic = os.getenv("CREATE_TOPIC", "True") == "True"
        self.p99_limit_ms = _env_int("P99_LIMIT_MS", 0)
        self.join_timeout_s = _env_int("JOIN_TIMEOUT_SECONDS", 120)
        self.settle_timeout_s = _env_int("SETTLE_TIMEOUT_SECONDS", 15)
        self.kafka_bin = os.getenv("KAFKA_BIN")  # set => self-spawn producer
        self.fetch_min_bytes = _env_int("FETCH_MIN_BYTES", 4 * 1024 * 1024)
        self.fetch_max_bytes = _env_int("MAX_PARTITION_FETCH_BYTES", 4 * 1024 * 1024)
        # Batch size per poll, applied to BOTH backends so they batch
        # identically: the Rust binding's max.poll.records and librdkafka's
        # consume(num_messages=...).
        self.batch_size = _env_int("CONSUMER_BATCH_SIZE", 2000)
        # Async mode toggle — same env var/convention as the producer perf test
        # (producer_performance_test.py). When set, the benchmark drives the
        # asyncio-native consumer of the selected CLIENT_VERSION.
        self.async_mode = os.getenv("ASYNC", "False") == "True"
        # When True, consume one message at a time: confluent_kafka's single
        # poll() (v2) / a poll_batch-backed single path (Rust v3), instead of
        # the batched consume()/poll().
        self.poll_single = os.getenv("POLL_SINGLE", "False") == "True"
        self.use_defaults = os.getenv("USE_DEFAULTS", "False") == "True"
        # Readiness gate before the timed window: require this many records with a
        # valid, non-negative latency to flow (feeder producing, consumer
        # receiving) before measuring; otherwise the run is retried.
        self.readiness_min_records = _env_int("READINESS_MIN_RECORDS", 20)
        self.readiness_timeout_s = _env_int("READINESS_TIMEOUT_SECONDS", 20)


# ---------------------------------------------------------------------------
# Consumer backends — normalized to a poll() yielding (timestamp_ms, nbytes).
# ---------------------------------------------------------------------------
class _RustConsumer:
    """bindings/python/consumer.py KafkaConsumer (CLIENT_VERSION=3)."""

    def __init__(self, cfg):
        from consumer import KafkaConsumer
        custom_conf = {} if cfg.use_defaults else {
            "fetch.min.bytes": str(cfg.fetch_min_bytes),
            "max.partition.fetch.bytes": str(cfg.fetch_max_bytes),
            "max.poll.records": str(cfg.batch_size + 500),
            "check.crcs": "false",
        }
        conf = {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "group.protocol": "consumer",
            "client.id": "rust-consumer-perf",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "true",
            **custom_conf
        }
        conf.update(sasl_config_from_env(v2=False))
        self._c = KafkaConsumer(conf)
        self._timeout = cfg.poll_timeout_ms / 1000.0

    def subscribe(self, topic):
        self._c.subscribe([topic])

    def assigned(self):
        try:
            return len(self._c.assignment()) > 0
        except Exception:
            return False

    def poll_batch(self):
        """Yield (timestamp_ms, nbytes) for each record in one poll."""
        records = self._c.poll(self._timeout)
        for r in records:
            value = r.value
            key = r.key
            nbytes = (len(value) if value is not None else 0) + (len(key) if key is not None else 0)
            yield r.timestamp, nbytes

    def poll_single(self):
        """POLL_SINGLE: the Rust binding exposes no single-message API, so reuse
        the existing batch poll (each record of one poll is yielded one by one)."""
        yield from self.poll_batch()

    def close(self):
        self._c.close()


class _LibrdkafkaConsumer:
    """confluent_kafka.Consumer (CLIENT_VERSION=2 baseline)."""

    def __init__(self, cfg):
        from confluent_kafka import Consumer
        custom_conf = {} if cfg.use_defaults else {
            "fetch.min.bytes": cfg.fetch_min_bytes,
            "fetch.message.max.bytes": cfg.fetch_max_bytes,
            "check.crcs": False,
        }
        conf = {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "group.protocol": "consumer",
            "client.id": "librdkafka-consumer-perf",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            **custom_conf
        }
        conf.update(sasl_config_from_env(v2=True))
        self._c = Consumer(conf)
        self._timeout = cfg.poll_timeout_ms / 1000.0
        self._batch = cfg.batch_size

    def subscribe(self, topic):
        self._c.subscribe([topic])

    def assigned(self):
        try:
            return len(self._c.assignment()) > 0
        except Exception:
            return False

    def poll_batch(self):
        msgs = self._c.consume(num_messages=self._batch, timeout=self._timeout)
        for msg in msgs:
            if msg is None or msg.error():
                continue
            _ts_type, ts = msg.timestamp()
            value = msg.value()
            key = msg.key()
            nbytes = (len(value) if value else 0) + (len(key) if key else 0)
            yield ts, nbytes

    def poll_single(self):
        """POLL_SINGLE: consume one message at a time via Consumer.poll()."""
        msg = self._c.poll(self._timeout)
        if msg is None or msg.error():
            return
        _ts_type, ts = msg.timestamp()
        value = msg.value()
        key = msg.key()
        nbytes = (len(value) if value else 0) + (len(key) if key else 0)
        yield ts, nbytes

    def close(self):
        self._c.close()


def build_consumer(cfg):
    return _RustConsumer(cfg) if cfg.client_version == "3" else _LibrdkafkaConsumer(cfg)


# ---------------------------------------------------------------------------
# Async backends (ASYNC=True) — same poll()->(timestamp_ms, nbytes) contract,
# but the blocking methods are coroutines. assigned()/poll_batch()/close() are
# async; poll_batch is an async generator.
# ---------------------------------------------------------------------------
class _AsyncRustConsumer:
    """bindings/python/consumer.py AsyncKafkaConsumer (CLIENT_VERSION=3)."""

    def __init__(self, cfg):
        from consumer import AsyncKafkaConsumer
        custom_conf = {} if cfg.use_defaults else {
            "fetch.min.bytes": str(cfg.fetch_min_bytes),
            "max.partition.fetch.bytes": str(cfg.fetch_max_bytes),
            "max.poll.records": str(cfg.batch_size + 500),
            "check.crcs": "false",
        }
        conf = {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "group.protocol": "consumer",
            "client.id": "rust-consumer-perf",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "true",
            **custom_conf
        }
        conf.update(sasl_config_from_env(v2=False))
        self._c = AsyncKafkaConsumer(conf)
        self._timeout = cfg.poll_timeout_ms / 1000.0

    async def subscribe(self, topic):
        await self._c.subscribe([topic])

    async def assigned(self):
        # assignment() is a sync (non-blocking) getter on the Rust consumer.
        try:
            return len(self._c.assignment()) > 0
        except Exception:
            return False

    async def poll_batch(self):
        records = await self._c.poll(self._timeout)
        for r in records:
            value = r.value
            key = r.key
            nbytes = (len(value) if value is not None else 0) + (len(key) if key is not None else 0)
            yield r.timestamp, nbytes

    async def poll_single(self):
        """POLL_SINGLE: no single-message API on the Rust async binding; reuse
        the existing batch poll."""
        async for x in self.poll_batch():
            yield x

    async def close(self):
        await self._c.close()


class _AsyncLibrdkafkaConsumer:
    """confluent_kafka.aio.AIOConsumer (CLIENT_VERSION=2 baseline)."""

    def __init__(self, cfg):
        from confluent_kafka.aio import AIOConsumer
        custom_conf = {} if cfg.use_defaults else {
            "fetch.min.bytes": cfg.fetch_min_bytes,
            "fetch.message.max.bytes": cfg.fetch_max_bytes,
            "check.crcs": False,
        }
        conf = {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "group.protocol": "consumer",
            "client.id": "librdkafka-consumer-perf",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            **custom_conf
        }
        conf.update(sasl_config_from_env(v2=True))
        self._c = AIOConsumer(conf)
        self._timeout = cfg.poll_timeout_ms / 1000.0
        self._batch = cfg.batch_size

    async def subscribe(self, topic):
        await self._c.subscribe([topic])

    async def assigned(self):
        # AIOConsumer.assignment() is a coroutine (unlike the Rust binding).
        try:
            return len(await self._c.assignment()) > 0
        except Exception:
            return False

    async def poll_batch(self):
        msgs = await self._c.consume(num_messages=self._batch, timeout=self._timeout)
        for msg in msgs:
            if msg is None or msg.error():
                continue
            _ts_type, ts = msg.timestamp()
            value = msg.value()
            key = msg.key()
            nbytes = (len(value) if value else 0) + (len(key) if key else 0)
            yield ts, nbytes

    async def poll_single(self):
        """POLL_SINGLE: consume one message at a time via AIOConsumer.poll()."""
        msg = await self._c.poll(self._timeout)
        if msg is None or msg.error():
            return
        _ts_type, ts = msg.timestamp()
        value = msg.value()
        key = msg.key()
        nbytes = (len(value) if value else 0) + (len(key) if key else 0)
        yield ts, nbytes

    async def close(self):
        await self._c.close()


def build_async_consumer(cfg):
    return _AsyncRustConsumer(cfg) if cfg.client_version == "3" \
        else _AsyncLibrdkafkaConsumer(cfg)


# ---------------------------------------------------------------------------
# Producer load (manual KAFKA_BIN path; in-suite produces in-container instead).
# ---------------------------------------------------------------------------
def spawn_producer(cfg, total_records):
    bin_path = os.path.join(cfg.kafka_bin, "kafka-producer-perf-test.sh")
    # The producer config goes through a properties file (--producer.config), not
    # --producer-props: ProducerPerformance.readProps() splits each --producer-props
    # token on "=" and rejects any value containing more than one "=" — which the
    # sasl.jaas.config value always does (username="..." password="...";). A Java
    # .properties file splits only on the first "=", so the jaas value stays intact.
    # SASL uses the Java form (security.protocol, sasl.mechanism, sasl.jaas.config)
    # so the Java producer authenticates against the same SASL broker as the consumer.
    props = {"bootstrap.servers": cfg.bootstrap_servers, "acks": "1"}
    props.update(sasl_config_from_env(v2=False))
    fd, props_path = tempfile.mkstemp(prefix="producer_perf_", suffix=".properties")
    with os.fdopen(fd, "w") as f:
        for k, v in props.items():
            # A Java .properties value must be one logical line: a raw newline
            # ends the entry. sasl.jaas.config from sasl_config_from_env carries
            # cosmetic "\n\t" between its terms, so collapse all whitespace runs
            # to single spaces before writing (the value is whitespace-insensitive).
            one_line = " ".join(str(v).split())
            f.write(f"{k}={one_line}\n")
    cmd = [
        bin_path,
        "--topic", cfg.topic,
        "--num-records", str(total_records),
        "--record-size", str(cfg.message_size),
        "--throughput", str(cfg.throughput),
        "--producer.config", props_path,
    ]
    print(f">>> Launching producer: throughput={cfg.throughput} msg/s, "
          f"{cfg.message_size} bytes, ~{total_records} records", flush=True)
    # Capture producer output to a file (cwd is the results dir) instead of
    # discarding it: a silent producer failure looks identical to "no data" on
    # the consumer side, so its stdout/stderr must remain inspectable.
    log = open("producer.log", "w")
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    # The caller removes this once the producer is killed (see run / run_async).
    proc.props_path = props_path
    return proc


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
_terminating = False


def _install_signal_handlers():
    def handler(signum, frame):
        global _terminating
        _terminating = True
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


class _Done(Exception):
    pass


def _print_header(cfg):
    mode = "async" if cfg.async_mode else "sync"
    poll_mode = "single" if cfg.poll_single else "batch"
    print("=" * 72)
    print(f"Consumer E2E Latency Benchmark - CLIENT_VERSION={cfg.client_version} ({mode}, poll={poll_mode})")
    print("=" * 72)
    print(f"Bootstrap: {cfg.bootstrap_servers}  Topic: {cfg.topic}  Group: {cfg.group_id}")
    print(f"Warmup: {cfg.warmup_seconds}s  Measure: {cfg.test_duration_seconds}s  "
          f"Interval: {cfg.interval_seconds}s  Poll: {cfg.poll_timeout_ms}ms")
    print("=" * 72, flush=True)


class _Measurement:
    """Shared loop state + bookkeeping for the sync ``run`` and async
    ``run_async``. Both feed one record at a time to ``process_record`` and
    consult the between-poll terminators, so the measurement semantics (warmup
    -> measure transition, latency histogram, num-messages / duration / no-data
    termination) live in exactly one place."""

    def __init__(self, cfg, metrics):
        self.cfg = cfg
        self.metrics = metrics
        # Overall latency histogram for the final summary (1 ms buckets +
        # overflow), same approach as producer_performance_test.py.
        self.latency_hist = [0] * (MAX_LATENCY_MS + 2)
        self.measured_messages = 0
        self.consume_start = None
        self.warmup_complete = (cfg.warmup_seconds <= 0)
        self.measure_start = None
        self.no_data_deadline = None

    def begin(self):
        self.metrics.start_collecting(interval_s=self.cfg.interval_seconds)
        if self.warmup_complete:
            self.measure_start = time.monotonic()
            self.metrics.measurement_start_ms = _now_ms()
        self.no_data_deadline = time.monotonic() + 120

    def process_record(self, ts_ms, nbytes):
        if self.consume_start is None:
            self.consume_start = time.monotonic()
        now = time.monotonic()

        if not self.warmup_complete:
            if now - self.consume_start >= self.cfg.warmup_seconds:
                self.warmup_complete = True
                self.measure_start = now
                self.metrics.measurement_start_ms = _now_ms()
                print(f">>> warmup complete ({self.cfg.warmup_seconds}s); measuring", flush=True)
            return

        if ts_ms and ts_ms > 0:
            latency = _now_ms() - ts_ms
            if latency >= 0:
                self.latency_hist[min(max(int(latency), 0), MAX_LATENCY_MS + 1)] += 1
                self.metrics.latency.add_measurement(latency)
                self.metrics.bytes.add_measurement(nbytes)
                self.metrics.messages.add_measurement(1)
                self.measured_messages += 1

        if self.cfg.num_messages > 0 and self.measured_messages >= self.cfg.num_messages:
            raise _Done()

    def time_limit_reached(self):
        return (self.warmup_complete and self.measure_start is not None
                and time.monotonic() - self.measure_start >= self.cfg.test_duration_seconds)

    def no_data_timeout(self):
        if not self.warmup_complete and self.consume_start is None \
                and time.monotonic() >= self.no_data_deadline:
            print("ERROR: no records within 120s (is something producing?)", file=sys.stderr)
            return True
        return False

    def measured_duration(self):
        return (time.monotonic() - self.measure_start) if self.measure_start else 0.0


# Returned by run()/run_async() when the pipeline never went live at the edge
# (feeder not producing yet, or no measurable records). Distinct from None (a
# hard error) so main() exits SETUP_NOT_READY_RC and the harness retries setup.
SETUP_NOT_READY = object()
SETUP_NOT_READY_RC = 2


def _measurable_latency_ms(ts_ms):
    """Latency (ms) if the record carries a usable CreateTime and the latency is
    non-negative; else None (not measurable)."""
    if ts_ms and ts_ms > 0:
        latency = _now_ms() - ts_ms
        if latency >= 0:
            return latency
    return None


def _readiness_ok(measurable, received, cfg):
    """True once enough measurable records have flowed; logs the outcome."""
    if measurable >= cfg.readiness_min_records:
        print(f">>> pipeline live ({measurable} records confirmed); measuring", flush=True)
        return True
    print(f"SETUP_NOT_READY: {measurable}/{cfg.readiness_min_records} measurable records "
          f"({received} received) in {cfg.readiness_timeout_s}s at live edge", file=sys.stderr)
    return False


def run(cfg, metrics=None):
    """Run the benchmark; return a stats dict. If `metrics` is None a fresh
    performance_common.Metrics (writing metrics.jsonl) is created."""
    _print_header(cfg)

    own_metrics = metrics is None
    if own_metrics:
        metrics = Metrics()

    # Optionally start from a clean topic before consuming (admin uses the
    # librdkafka v2-form SASL config regardless of CLIENT_VERSION).
    if cfg.create_topic:
        recreate_topic(cfg.bootstrap_servers, cfg.topic,
                       sasl_config_from_env(v2=True), cfg.partitions)

    consumer = build_consumer(cfg)
    consumer.subscribe(cfg.topic)
    # Single-message vs batched consume (POLL_SINGLE), used for settle + measure.
    poll = consumer.poll_single if cfg.poll_single else consumer.poll_batch

    # Wait for partition assignment.
    join_start = time.monotonic()
    while not consumer.assigned():
        list(poll())
        if time.monotonic() - join_start >= cfg.join_timeout_s:
            print("ERROR: timed out waiting for assignment", file=sys.stderr)
            consumer.close()
            return None
    print(f">>> assigned after {time.monotonic() - join_start:.1f}s", flush=True)

    # Settle to the live edge (poll until two consecutive empty polls), so we
    # only measure records produced after we are caught up — mirrors the C/Rust.
    settle_deadline = time.monotonic() + cfg.settle_timeout_s
    empties = 0
    while empties < 2 and time.monotonic() < settle_deadline:
        got = list(poll())
        empties = empties + 1 if not got else 0
    print(">>> at live edge", flush=True)

    producer = None
    if cfg.kafka_bin:
        pad = cfg.warmup_seconds + cfg.test_duration_seconds + 30
        total = cfg.num_messages if cfg.num_messages > 0 else cfg.throughput * pad
        producer = spawn_producer(cfg, total)

    # Readiness gate before the timed window (see _readiness_ok). Records
    # consumed here are warmup, not measured.
    ready_deadline = time.monotonic() + cfg.readiness_timeout_s
    ready_measurable = ready_received = 0
    while ready_measurable < cfg.readiness_min_records and time.monotonic() < ready_deadline:
        for ts_ms, _nbytes in poll():
            ready_received += 1
            if _measurable_latency_ms(ts_ms) is not None:
                ready_measurable += 1
    if not _readiness_ok(ready_measurable, ready_received, cfg):
        consumer.close()
        return SETUP_NOT_READY

    meas = _Measurement(cfg, metrics)
    meas.begin()
    try:
        while not _terminating:
            for ts_ms, nbytes in poll():
                meas.process_record(ts_ms, nbytes)
            if meas.time_limit_reached():
                break
            if meas.no_data_timeout():
                break
    except _Done:
        pass
    finally:
        measured_duration = meas.measured_duration()
        metrics.measurement_end_ms = _now_ms()
        if producer is not None:
            try:
                producer.kill()
                producer.wait(timeout=5)
            except Exception:
                pass
            try:
                os.unlink(producer.props_path)
            except OSError:
                pass
        consumer.close()
        if own_metrics:
            metrics.stop_collecting()

    return _summarize(cfg, meas.latency_hist, meas.measured_messages, measured_duration,
                      metrics)


async def run_async(cfg, metrics=None):
    """Async counterpart of ``run`` (ASYNC=True): drives the asyncio-native
    consumer of the selected CLIENT_VERSION on one event loop. Phase-for-phase
    identical to ``run`` but awaiting, and sharing ``_Measurement`` /
    ``_summarize`` so the stats and output are the same."""
    _print_header(cfg)

    own_metrics = metrics is None
    if own_metrics:
        metrics = Metrics()

    # Clean topic before consuming. recreate_topic is synchronous (admin +
    # sleeps); it runs once at setup before the consumer touches the loop.
    if cfg.create_topic:
        recreate_topic(cfg.bootstrap_servers, cfg.topic,
                       sasl_config_from_env(v2=True), cfg.partitions)

    consumer = build_async_consumer(cfg)
    await consumer.subscribe(cfg.topic)
    # Single-message vs batched consume (POLL_SINGLE), used for settle + measure.
    poll = consumer.poll_single if cfg.poll_single else consumer.poll_batch

    # Wait for partition assignment.
    join_start = time.monotonic()
    while not await consumer.assigned():
        async for _ in poll():
            pass
        if time.monotonic() - join_start >= cfg.join_timeout_s:
            print("ERROR: timed out waiting for assignment", file=sys.stderr)
            await consumer.close()
            return None
    print(f">>> assigned after {time.monotonic() - join_start:.1f}s", flush=True)

    # Settle to the live edge (two consecutive empty polls).
    settle_deadline = time.monotonic() + cfg.settle_timeout_s
    empties = 0
    while empties < 2 and time.monotonic() < settle_deadline:
        got = False
        async for _ in poll():
            got = True
        empties = empties + 1 if not got else 0
    print(">>> at live edge", flush=True)

    producer = None
    if cfg.kafka_bin:
        pad = cfg.warmup_seconds + cfg.test_duration_seconds + 30
        total = cfg.num_messages if cfg.num_messages > 0 else cfg.throughput * pad
        producer = spawn_producer(cfg, total)

    # Readiness gate before the timed window (see _readiness_ok). Records
    # consumed here are warmup, not measured.
    ready_deadline = time.monotonic() + cfg.readiness_timeout_s
    ready_measurable = ready_received = 0
    while ready_measurable < cfg.readiness_min_records and time.monotonic() < ready_deadline:
        async for ts_ms, _nbytes in poll():
            ready_received += 1
            if _measurable_latency_ms(ts_ms) is not None:
                ready_measurable += 1
    if not _readiness_ok(ready_measurable, ready_received, cfg):
        await consumer.close()
        return SETUP_NOT_READY

    meas = _Measurement(cfg, metrics)
    meas.begin()
    try:
        while not _terminating:
            async for ts_ms, nbytes in poll():
                meas.process_record(ts_ms, nbytes)
            if meas.time_limit_reached():
                break
            if meas.no_data_timeout():
                break
    except _Done:
        pass
    finally:
        measured_duration = meas.measured_duration()
        metrics.measurement_end_ms = _now_ms()
        if producer is not None:
            try:
                producer.kill()
                producer.wait(timeout=5)
            except Exception:
                pass
            try:
                os.unlink(producer.props_path)
            except OSError:
                pass
        await consumer.close()
        if own_metrics:
            metrics.stop_collecting()

    return _summarize(cfg, meas.latency_hist, meas.measured_messages, measured_duration,
                      metrics)


def _summarize(cfg, latency_hist, measured_messages, measured_duration,
               metrics=None):
    p50 = percentile_from_hist(latency_hist, 0.50)
    p90 = percentile_from_hist(latency_hist, 0.90)
    p95 = percentile_from_hist(latency_hist, 0.95)
    p99 = percentile_from_hist(latency_hist, 0.99)
    p999 = percentile_from_hist(latency_hist, 0.999)
    total = sum(latency_hist)
    avg = (sum(ms * c for ms, c in enumerate(latency_hist)) / total) if total else 0.0
    mn = next((ms for ms, c in enumerate(latency_hist) if c), 0)
    mx = next((ms for ms in range(len(latency_hist) - 1, -1, -1) if latency_hist[ms]), 0)
    thr_msg = (measured_messages / measured_duration) if measured_duration > 0 else 0.0
    thr_mib = ((measured_messages * cfg.message_size) / (1024 * 1024) / measured_duration
               if measured_duration > 0 else 0.0)
    stats = {
        "client_version": cfg.client_version, "topic": cfg.topic,
        "messages_measured": measured_messages, "duration_s": round(measured_duration, 2),
        "throughput_msg_s": round(thr_msg, 2), "throughput_mib_s": round(thr_mib, 2),
        "latency_ms": {"min": mn, "avg": round(avg, 2), "p50": p50, "p90": p90,
                       "p95": p95, "p99": p99, "p999": p999, "max": mx},
    }
    # CPU/RSS averages over the measured interval, from the shared Metrics
    # sampler (psutil, per-interval buckets already written to metrics.jsonl).
    # Same keys as the other perf tests' results.json.
    cpu_avg = rss_avg_mb = None
    if metrics is not None and metrics.total_external_metrics > 0:
        n = metrics.total_external_metrics
        cpu_avg = metrics.total_cpu / n
        rss_avg_mb = metrics.total_rss / n / (1024.0 * 1024.0)
        stats["cpu_avg_pct"] = round(cpu_avg, 2)
        stats["rss_avg_mb"] = round(rss_avg_mb, 2)
    print("\n" + "=" * 72)
    print(f"SUMMARY - CLIENT_VERSION={cfg.client_version} (warmup excluded)")
    print("=" * 72)
    print(f"Measured messages: {measured_messages}")
    print(f"Duration:          {measured_duration:.2f} s")
    print(f"Throughput:        {thr_msg:.0f} msg/s  ({thr_mib:.2f} MiB/s)")
    print(f"E2E latency (ms):  min={mn} avg={avg:.2f} p50={p50} p90={p90} "
          f"p95={p95} p99={p99} p99.9={p999} max={mx}")
    if cpu_avg is not None:
        print(f"CPU (% one core):  avg={cpu_avg:.1f}")
        print(f"RSS (MB):          avg={rss_avg_mb:.1f}")
    print("=" * 72, flush=True)
    try:
        with open("results.json", "w") as fh:
            json.dump(stats, fh, indent=2)
    except OSError:
        pass
    return stats


def main():
    _install_signal_handlers()
    cfg = Config()
    stats = asyncio.run(run_async(cfg)) if cfg.async_mode else run(cfg)
    if stats is SETUP_NOT_READY:
        return SETUP_NOT_READY_RC
    if stats is None:
        return 1
    p99 = stats["latency_ms"]["p99"]
    if cfg.p99_limit_ms > 0 and p99 > cfg.p99_limit_ms:
        print(f"FAIL: p99 latency {p99} ms exceeds budget {cfg.p99_limit_ms} ms", file=sys.stderr)
        return 1
    if stats["messages_measured"] <= 0:
        print("FAIL: no messages measured", file=sys.stderr)
        return 1
    return 0


# ---------------------------------------------------------------------------
# In-suite pytest entry (short run + assertions, testcontainers broker).
# ---------------------------------------------------------------------------
def _smoke_env(kafka_broker, topic, extra=None):
    """Short in-suite benchmark env shared by the sync and async pytest cases.

    Matches the Rust automatic perf test's in-suite config
    (tests/integration/producer_perf_test.rs): 100 rps, 10 s, p99<=70 ms, no
    warmup, 2048-byte values. SETTLE_TIMEOUT_SECONDS is consumer-specific (kept
    short so the low-rate live-edge settle doesn't dominate the run).

    FETCH_MIN_BYTES=1 is set for the in-suite run only: the C benchmark's 4 MiB
    floor never fills at 100 rps, so fetches would block on fetch.max.wait.ms
    (~500 ms) and dominate e2e latency. With a 1-byte floor the broker returns
    as soon as a record is available, so the 70 ms budget measures the pipeline
    rather than fetch batching. The manual/full benchmark keeps the C-faithful
    4 MiB default.
    """
    env = dict(os.environ)
    env.update({
        "BOOTSTRAP_SERVERS": kafka_broker.external_bootstrap,
        "TOPIC_NAME": topic,
        "CLIENT_VERSION": "3",
        "WARMUP_SECONDS": "0",
        "TEST_DURATION_SECONDS": "10",
        "INTERVAL_SECONDS": "1",
        "POLL_TIMEOUT_MS": "500",
        "VALUE_SIZE": "2048",
        "FETCH_MIN_BYTES": "1",
        # Inherit a looser P99_LIMIT_MS if set (e.g. the macOS run); 0 disables
        # the latency assert, default 70 keeps the Linux budget unchanged.
        "P99_LIMIT_MS": os.getenv("P99_LIMIT_MS", "70"),
        "JOIN_TIMEOUT_SECONDS": "60",
        "SETTLE_TIMEOUT_SECONDS": "5",
        # The fixture creates the topic via testcontainers; skip the
        # delete+recreate (and its 20s of sleeps) for the in-suite run.
        "CREATE_TOPIC": "False",
    })
    if extra:
        env.update(extra)
    env.pop("KAFKA_BIN", None)  # consume-only; load comes from the container
    return env


def _run_consumer_smoke(kafka_broker, topic, extra=None):
    """Create the topic, then for up to PERF_SETUP_ATTEMPTS tries: drive a steady
    100 msg/s in-container producer, run this script as a subprocess, and assert
    it succeeds within the p99 budget. A SETUP_NOT_READY_RC exit means the
    pipeline never went live (a slow feeder under a virtualized Docker host), so
    the feeder is restarted and the run retried rather than failed."""
    import conftest

    conftest.create_topic(kafka_broker, topic, partitions=4)
    env = _smoke_env(kafka_broker, topic, extra)
    attempts = _env_int("PERF_SETUP_ATTEMPTS", 3)

    proc = None
    for attempt in range(1, attempts + 1):
        producer = conftest.produce_perf_in_container(
            kafka_broker, topic, num_records=10000, record_size=2048, throughput=100)
        try:
            proc = subprocess.run(
                [sys.executable, os.path.abspath(__file__)],
                env=env, cwd=os.path.dirname(os.path.abspath(__file__)),
                timeout=180, capture_output=True, text=True)
        finally:
            producer.stop()

        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        if proc.returncode == SETUP_NOT_READY_RC and attempt < attempts:
            print(f">>> pipeline not ready (attempt {attempt}/{attempts}); "
                  f"restarting feeder and retrying", flush=True)
            continue
        break

    assert proc.returncode == 0, (
        f"consumer perf run failed (rc={proc.returncode}) after {attempt} attempt(s); "
        f"see output above")


# E2E consumer latency is (host consumer clock) - (producer CreateTime). On macOS
# the feeder runs in Colima's VM, whose clock drifts from the host, so live-record
# latencies go negative and become unmeasurable; the measurement is only valid
# where the broker container shares the host clock (Linux).
_SKIP_CROSS_CLOCK = sys.platform == "darwin"
_CROSS_CLOCK_REASON = (
    "consumer e2e latency is unmeasurable across the Colima VM/host clock boundary "
    "on macOS; runs on Linux where the broker container shares the host clock"
)


@pytest.mark.skipif(_SKIP_CROSS_CLOCK, reason=_CROSS_CLOCK_REASON)
def test_consumer_e2e_latency(kafka_broker):
    """Sync consumer e2e-latency smoke run against a testcontainers broker.

    Skips (via the fixture) when Docker/testcontainers is unavailable.
    """
    _run_consumer_smoke(kafka_broker, "consumer-perf-smoke")


@pytest.mark.skipif(_SKIP_CROSS_CLOCK, reason=_CROSS_CLOCK_REASON)
def test_consumer_e2e_latency_async(kafka_broker):
    """Async (ASYNC=True) consumer e2e-latency smoke run — same config + budget
    as the sync case, exercising AsyncKafkaConsumer.
    """
    _run_consumer_smoke(kafka_broker, "consumer-perf-smoke-async", {"ASYNC": "True"})


if __name__ == "__main__":
    sys.exit(main())
