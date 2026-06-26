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

"""Consumer end-to-end latency benchmark, mirroring
``consumer-perf/compare/benchmark_e2e_latency.c`` in detail.

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

import json
import os
import signal
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_BINDINGS = os.path.dirname(os.path.dirname(_HERE))  # bindings/python (consumer.py, _confluentkafka)
for _p in (_HERE, _BINDINGS):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from performance_common import Metrics, MAX_LATENCY_MS, percentile_from_hist  # noqa: E402


def _now_ms():
    return int(time.time() * 1000)


def _env_int(name, default):
    v = os.getenv(name)
    return int(v) if v is not None and v != "" else default


class Config:
    """Benchmark configuration, parsed from the environment (defaults mirror
    benchmark_e2e_latency.c)."""

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
        self.message_size = _env_int("VALUE_SIZE", _env_int("MESSAGE_SIZE", 1024))
        self.throughput = _env_int("THROUGHPUT", 100000)  # producer msg/s (KAFKA_BIN path)
        self.num_messages = _env_int("NUM_MESSAGES", 0)  # 0 => duration-based
        self.partitions = _env_int("PARTITIONS", 1)
        self.p99_limit_ms = _env_int("P99_LIMIT_MS", 0)
        self.join_timeout_s = _env_int("JOIN_TIMEOUT_SECONDS", 120)
        self.settle_timeout_s = _env_int("SETTLE_TIMEOUT_SECONDS", 15)
        self.kafka_bin = os.getenv("KAFKA_BIN")  # set => self-spawn producer
        self.fetch_min_bytes = _env_int("FETCH_MIN_BYTES", 4 * 1024 * 1024)
        self.fetch_max_bytes = _env_int("MAX_PARTITION_FETCH_BYTES", 4 * 1024 * 1024)


# ---------------------------------------------------------------------------
# Consumer backends — normalized to a poll() yielding (timestamp_ms, nbytes).
# ---------------------------------------------------------------------------
class _RustConsumer:
    """bindings/python/consumer.py KafkaConsumer (CLIENT_VERSION=3)."""

    def __init__(self, cfg):
        from consumer import KafkaConsumer
        conf = {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "group.protocol": "consumer",
            "client.id": "rust-consumer-perf",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "true",
            "fetch.min.bytes": str(cfg.fetch_min_bytes),
            "max.partition.fetch.bytes": str(cfg.fetch_max_bytes),
        }
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

    def close(self):
        self._c.close()


class _LibrdkafkaConsumer:
    """confluent_kafka.Consumer (CLIENT_VERSION=2 baseline)."""

    def __init__(self, cfg):
        from confluent_kafka import Consumer
        conf = {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "group.protocol": "consumer",
            "client.id": "librdkafka-consumer-perf",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "fetch.min.bytes": cfg.fetch_min_bytes,
            "fetch.message.max.bytes": cfg.fetch_max_bytes,
            "check.crcs": False,
        }
        self._c = Consumer(conf)
        self._timeout = cfg.poll_timeout_ms / 1000.0

    def subscribe(self, topic):
        self._c.subscribe([topic])

    def assigned(self):
        try:
            return len(self._c.assignment()) > 0
        except Exception:
            return False

    def poll_batch(self):
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
# Producer load (manual KAFKA_BIN path; in-suite produces in-container instead).
# ---------------------------------------------------------------------------
def spawn_producer(cfg, total_records):
    bin_path = os.path.join(cfg.kafka_bin, "kafka-producer-perf-test.sh")
    cmd = [
        bin_path,
        "--topic", cfg.topic,
        "--num-records", str(total_records),
        "--record-size", str(cfg.message_size),
        "--throughput", str(cfg.throughput),
        "--producer-props", f"bootstrap.servers={cfg.bootstrap_servers}", "acks=1",
    ]
    print(f">>> Launching producer: throughput={cfg.throughput} msg/s, "
          f"{cfg.message_size} bytes, ~{total_records} records", flush=True)
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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


def run(cfg, metrics=None):
    """Run the benchmark; return a stats dict. If `metrics` is None a fresh
    performance_common.Metrics (writing metrics.jsonl) is created."""
    print("=" * 72)
    print(f"Consumer E2E Latency Benchmark - CLIENT_VERSION={cfg.client_version}")
    print("=" * 72)
    print(f"Bootstrap: {cfg.bootstrap_servers}  Topic: {cfg.topic}  Group: {cfg.group_id}")
    print(f"Warmup: {cfg.warmup_seconds}s  Measure: {cfg.test_duration_seconds}s  "
          f"Interval: {cfg.interval_seconds}s  Poll: {cfg.poll_timeout_ms}ms")
    print("=" * 72, flush=True)

    own_metrics = metrics is None
    if own_metrics:
        metrics = Metrics()

    consumer = build_consumer(cfg)
    consumer.subscribe(cfg.topic)

    # Wait for partition assignment.
    join_start = time.monotonic()
    while not consumer.assigned():
        list(consumer.poll_batch())
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
        got = list(consumer.poll_batch())
        empties = empties + 1 if not got else 0
    print(">>> at live edge", flush=True)

    producer = None
    if cfg.kafka_bin:
        pad = cfg.warmup_seconds + cfg.test_duration_seconds + 30
        total = cfg.num_messages if cfg.num_messages > 0 else cfg.throughput * pad
        producer = spawn_producer(cfg, total)

    # Overall latency histogram for the final summary (1 ms buckets + overflow),
    # same approach as producer_performance_test.py.
    latency_hist = [0] * (MAX_LATENCY_MS + 2)
    measured_messages = 0
    consume_start = None
    warmup_complete = (cfg.warmup_seconds <= 0)
    measure_start = None
    metrics.start_collecting(interval_s=cfg.interval_seconds)
    if warmup_complete:
        measure_start = time.monotonic()
        metrics.measurement_start_ms = _now_ms()

    no_data_deadline = time.monotonic() + 120
    try:
        while not _terminating:
            for ts_ms, nbytes in consumer.poll_batch():
                if consume_start is None:
                    consume_start = time.monotonic()
                now = time.monotonic()

                if not warmup_complete:
                    if now - consume_start >= cfg.warmup_seconds:
                        warmup_complete = True
                        measure_start = now
                        metrics.measurement_start_ms = _now_ms()
                        print(f">>> warmup complete ({cfg.warmup_seconds}s); measuring", flush=True)
                    continue

                if ts_ms and ts_ms > 0:
                    latency = _now_ms() - ts_ms
                    if latency >= 0:
                        latency_hist[min(max(int(latency), 0), MAX_LATENCY_MS + 1)] += 1
                        metrics.latency.add_measurement(latency)
                        metrics.bytes.add_measurement(nbytes)
                        metrics.messages.add_measurement(1)
                        measured_messages += 1

                if cfg.num_messages > 0 and measured_messages >= cfg.num_messages:
                    raise _Done()

            # Termination / no-data checks between polls.
            if warmup_complete and measure_start is not None \
                    and time.monotonic() - measure_start >= cfg.test_duration_seconds:
                break
            if not warmup_complete and consume_start is None \
                    and time.monotonic() >= no_data_deadline:
                print("ERROR: no records within 120s (is something producing?)", file=sys.stderr)
                break
    except _Done:
        pass
    finally:
        measured_duration = (time.monotonic() - measure_start) if measure_start else 0.0
        metrics.measurement_end_ms = _now_ms()
        if producer is not None:
            try:
                producer.kill()
                producer.wait(timeout=5)
            except Exception:
                pass
        consumer.close()
        if own_metrics:
            metrics.stop_collecting()

    stats = _summarize(cfg, latency_hist, measured_messages, measured_duration)
    return stats


class _Done(Exception):
    pass


def _summarize(cfg, latency_hist, measured_messages, measured_duration):
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
    print("\n" + "=" * 72)
    print(f"SUMMARY - CLIENT_VERSION={cfg.client_version} (warmup excluded)")
    print("=" * 72)
    print(f"Measured messages: {measured_messages}")
    print(f"Duration:          {measured_duration:.2f} s")
    print(f"Throughput:        {thr_msg:.0f} msg/s  ({thr_mib:.2f} MiB/s)")
    print(f"E2E latency (ms):  min={mn} avg={avg:.2f} p50={p50} p90={p90} "
          f"p95={p95} p99={p99} p99.9={p999} max={mx}")
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
    stats = run(cfg)
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
def test_consumer_e2e_latency(kafka_broker):
    """Short e2e-latency smoke run against a testcontainers Kafka broker.

    Produces a burst inside the broker container, then runs this script
    consume-only (no KAFKA_BIN) with a short measurement window + p99 budget,
    asserting it measures messages within budget. Skips (via the fixture) when
    Docker/testcontainers is unavailable.
    """
    import conftest

    topic = "consumer-perf-smoke"
    conftest.create_topic(kafka_broker, topic, partitions=4)

    env = dict(os.environ)
    env.update({
        "BOOTSTRAP_SERVERS": kafka_broker.external_bootstrap,
        "TOPIC_NAME": topic,
        "CLIENT_VERSION": "3",
        "WARMUP_SECONDS": "0",
        "TEST_DURATION_SECONDS": "8",
        "INTERVAL_SECONDS": "1",
        "POLL_TIMEOUT_MS": "500",
        "VALUE_SIZE": "256",
        "P99_LIMIT_MS": "5000",
        "JOIN_TIMEOUT_SECONDS": "60",
    })
    env.pop("KAFKA_BIN", None)  # consume-only; load comes from the container

    # Produce a steady burst inside the container while the consumer measures.
    producer = conftest.produce_perf_in_container(
        kafka_broker, topic, num_records=40000, record_size=256, throughput=2000)
    try:
        proc = subprocess.run(
            [sys.executable, os.path.abspath(__file__)],
            env=env, cwd=os.path.dirname(os.path.abspath(__file__)),
            timeout=180, capture_output=True, text=True)
    finally:
        producer.stop()

    print(proc.stdout)
    print(proc.stderr, file=sys.stderr)
    assert proc.returncode == 0, (
        f"consumer perf run failed (rc={proc.returncode}); see output above")


if __name__ == "__main__":
    sys.exit(main())
