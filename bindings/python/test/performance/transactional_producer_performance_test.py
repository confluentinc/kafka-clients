# Copyright 2026 Confluent Inc.
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

"""Transactional producer performance test (Python).

Sibling of ``producer_performance_test.py``, driving the transactional producer
API. It shares the configuration contract, message shape, warmup/measured/
cooldown scaffolding, 100 ms rate pacing, CPU/RSS aggregation and the
``metrics.jsonl`` / ``results.json`` schema of the sibling transactional
producer performance tests
(``tests/performance/transactional_producer_perf_test.rs``,
``bindings/c/tests/transactional_producer_perf_test.c`` and
``tools/java-perf-test/.../TransactionalProducerPerformanceTest.java``) so
results are comparable across implementations, plus the transaction-specific
schema additions (``committed_transactions`` / ``aborted_transactions`` /
``aborted_records`` / ``transactions_per_s`` / ``commit_latency_ms`` /
``abort_latency_ms``, and the ``transactions`` / ``commit_latency`` /
``abort_latency`` metrics.jsonl buckets).

Backends (``CLIENT_VERSION`` env, matching the base harness):
  * ``v3`` (default) = the Python Rust binding ``producer.KafkaProducer``;
  * ``v2`` = ``confluent_kafka.Producer`` (librdkafka baseline).

Two modes (``TXN_MODE`` env):
  * ``produce`` (default) — per producer worker: ``begin`` -> produce
    ``RECORDS_PER_TRANSACTION`` -> ``commit``/``abort`` per the deterministic
    (Bresenham) ``ABORT_RATE`` spread. ``NUM_TRANSACTIONAL_PRODUCERS`` worker
    threads run concurrently.
  * ``eos`` — a read-process-write (exactly-once) pipeline: each worker's
    consumer polls ``SOURCE_TOPIC`` -> ``begin`` -> produce transformed
    (identity/echo) records to ``TOPIC_NAME`` -> ``send_offsets_to_transaction``
    (last-consumed-offset + 1 per partition, with the consumer group metadata)
    -> ``commit``/``abort``. An empty poll is skipped (no empty transaction), so
    a run with no source data terminates on the duration / message bound.

Latency definitions (both modes, matching the native harnesses):
  * per-record latency = a record's produce -> the moment its transaction's
    commit completes (committed transactions only);
  * per-transaction commit latency = ``begin`` -> ``commit`` completes
    (committed only);
  * per-transaction abort latency = ``begin`` -> ``abort`` completes
    (deterministic-abort path only; symmetric with commit latency). Emitted as
    ``abort_latency_ms`` in results.json and an ``abort_latency`` bucket per
    metrics.jsonl window (Kaushik: "Should we track abort latencies also?").

EOS source seeding (Kaushik C:1612): the EOS pipeline's throughput ceiling is
``min(source-produce-rate, txn-process-rate)``, so ``SOURCE_TOPIC`` must hold
enough records for the whole measured window or the consumer starves and txn
throughput is understated. The CANONICAL mechanism — identical across all four
transactional harnesses (Rust / librdkafka-C / Java / Python) — is to spawn
Kafka's standard ``kafka-producer-perf-test.sh`` (a plain Java producer) from
``$KAFKA_BIN`` BEFORE the measured interval, exactly as the consumer perf tests
seed their input load (``bindings/python/test/performance/consumer_performance_test.py``
``spawn_producer``, ``consumer-perf/src/main.rs``). It runs at peak
(``--throughput -1``) by default with ``--num-records`` sized to cover the window
(``SEED_COUNT`` / ``SEED_THROUGHPUT`` override), forwarding SASL through a
``--producer.config`` properties file. Seed-then-run: the harness waits for the
seeder to finish. When ``KAFKA_BIN`` is unset the source is assumed externally
pre-populated (this Python harness has no in-suite eos seeding; its pytest smoke
runs produce mode). The consumer
never hangs on an under-fed source: the poll is bounded (``EOS_POLL_TIMEOUT_MS``)
and an empty poll is skipped rather than opening an empty transaction, with a
one-time "source starved" warning.

KIP-848 (Kaushik C:128): ALL consumers across the transactional perf harnesses
run on the KIP-848 consumer group protocol, uniformly across Rust / librdkafka-C
/ Java / Python. Both the python-rust (KIP-848-only) and the python-librdkafka
EOS consumers set ``group.protocol=consumer``. This requires a KIP-848-capable
broker (Kafka 4.x) and, for the librdkafka backend, a KIP-848-capable client
(librdkafka >= 2.5).

Two ways to run:
  * As part of the pytest perf suite (short, asserted, Docker broker):
    ``python -m pytest test/performance/transactional_producer_performance_test.py``
  * As an env-driven benchmark (long runs, external broker):
    ``python transactional_producer_performance_test.py`` (set env vars first),
    or via the ``make transactional-producer-perf-test-python`` target.
"""

import asyncio
import datetime
import gc
import json
import math
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from threading import Lock, Thread

from performance_common import (Bucket, LatencyBucket, Metrics, MAX_LATENCY_MS,
                                percentile_from_hist, recreate_topic)

# --------------------------------------------------------------------------
# Configuration (env), mirroring producer_performance_test.py plus the
# transaction-specific knobs.
# --------------------------------------------------------------------------
terminating = False

topic_name = os.getenv("TOPIC_NAME", "test-topic")
key_size = int(os.getenv("KEY_SIZE", "0"))
value_size = int(os.getenv("VALUE_SIZE", "2048"))
# Computed AFTER the KEY_SIZE/VALUE_SIZE env overrides (same as the base
# harness): message_size feeds the bytes-per-message metrics and MiB/s summary.
message_size = key_size + value_size

v2 = os.getenv("CLIENT_VERSION", "3") == "2"
# ASYNC=True drives the async transaction API (v3 AsyncKafkaProducer /
# v2 confluent_kafka.aio.AIOProducer) with awaited control ops + async sends,
# exercising the async drain-before-control-ops path. Default False = sync.
run_async = os.getenv("ASYNC", "False") == "True"
do_verify = os.getenv("DO_VERIFY", "True") == "True"
use_defaults = os.getenv("USE_DEFAULTS", "False") == "True"
create_topic = os.getenv("CREATE_TOPIC", "True") == "True"
partitions = int(os.getenv("PARTITIONS", "-1"))
warmup_s = int(os.getenv("WARMUP_SECONDS", "120"))
test_duration_s = int(os.getenv("TEST_DURATION_SECONDS", "600"))

limit_rps = os.getenv("LIMIT_RPS", None)
if limit_rps is not None:
    limit_rps = int(limit_rps)
    # LIMIT_RPS <= 0 means unbounded (max rate), matching the Rust/C/Java tests.
    if limit_rps <= 0:
        limit_rps = None

num_messages = int(os.getenv("NUM_MESSAGES", "0"))
if limit_rps is not None:
    num_messages = int(limit_rps * test_duration_s)

# p99 per-record latency budget (ms); 0 disables the assertion. Per-record
# latency in transactional produce mode is dominated by transaction-fill time
# (RECORDS_PER_TRANSACTION / rate), so it defaults off — the meaningful
# transactional signal is commit_latency_ms.
p99_limit_ms = int(os.getenv("P99_LIMIT_MS", "0"))
results_file = os.getenv("RESULTS_FILE", "results.json")
# Seconds to keep collecting metrics after the measured interval (cooldown).
POST_TEST_AWAIT_SECONDS = 10

# --- transaction-specific knobs ---
records_per_transaction = max(1, int(os.getenv("RECORDS_PER_TRANSACTION", "100")))
abort_rate = min(1.0, max(0.0, float(os.getenv("ABORT_RATE", "0.0"))))
num_transactional_producers = max(1, int(os.getenv("NUM_TRANSACTIONAL_PRODUCERS", "1")))
transactional_id = os.getenv("TRANSACTIONAL_ID", "perf-txn")
txn_mode = os.getenv("TXN_MODE", "produce")
eos_mode = txn_mode == "eos"

# --- EOS-mode knobs (TXN_MODE=eos) ---
source_topic = os.getenv("SOURCE_TOPIC", None)
# Consumer group id shared across all producers' consumers, so the coordinator
# divides the source partitions among them (canonical EOS scaling).
group_id = os.getenv("GROUP_ID", f"{transactional_id}-eos-consumer")
# Per-poll timeout (ms): bounds each poll so a run with no source data cannot
# block forever.
EOS_POLL_TIMEOUT_MS = 500
# Canonical EOS source-topic seeding (same mechanism as the consumer perf tests):
# when KAFKA_BIN is set, self-seed SOURCE_TOPIC before the measured interval by
# spawning Kafka's standard kafka-producer-perf-test.sh (a plain Java producer),
# identical across all four transactional harnesses. Unset => SOURCE_TOPIC is
# assumed externally pre-populated (this harness has no in-suite eos seeding).
kafka_bin = os.getenv("KAFKA_BIN", None)
# Seed producer --throughput (-1 = peak/unbounded; a positive value caps the rate)
# and --num-records (default sized to cover the whole window).
seed_throughput = int(os.getenv("SEED_THROUGHPUT", "-1"))
seed_count_env = os.getenv("SEED_COUNT", None)

version_str = "v2 (confluent-kafka)" if v2 else "v3 (confluent-kafka-rust)"

# Pre-generate the message corpus (shared across producers), like the base test.
GENERATED_MESSAGE_COUNT = 10000


def message_generator(key_sz, value_sz, n=GENERATED_MESSAGE_COUNT, randomness=0.5):
    """Constant prefix + random suffix, no key when key_sz == 0. Mirrors the
    base producer_performance_test.py generator."""
    ret = []
    rand_bytes = int(value_sz * randomness)
    constant_bytes = random.randbytes(value_sz - rand_bytes)
    key_constant_bytes = None
    key_rand_bytes = 0
    if key_sz > 0:
        key_rand_bytes = int(key_sz * randomness)
        key_constant_bytes = random.randbytes(key_sz - key_rand_bytes)
    for _ in range(n):
        key = None
        if key_sz > 0:
            key = key_constant_bytes + random.randbytes(key_rand_bytes)
        value = constant_bytes + random.randbytes(rand_bytes)
        ret.append((key, value))
    return ret


generated_messages = message_generator(key_size, value_size)


# --------------------------------------------------------------------------
# Metrics: performance_common.Metrics + the transaction-specific per-window
# buckets, mirroring Java's TransactionalMetrics. The extra keys are appended
# after the standard ones, so metrics.jsonl stays backward compatible
# (plot_metrics.py ignores unknown fields).
# --------------------------------------------------------------------------
class TransactionalMetrics(Metrics):
    def __init__(self):
        super().__init__()
        self.transactions = Bucket()          # committed transactions this window
        self.commit_latency = LatencyBucket()  # begin -> commit (committed only)
        self.abort_latency = LatencyBucket()   # begin -> abort (deterministic only)

    def rollover(self):
        # super().rollover() emits + resets rss/cpu/latency/bytes/messages and
        # the window/measurement markers; swap-and-append the txn buckets the
        # same way (so a measurement arriving mid-rollover lands in the fresh
        # bucket rather than being double-counted).
        result = super().rollover()
        transactions, self.transactions = self.transactions, Bucket()
        commit_latency, self.commit_latency = self.commit_latency, LatencyBucket()
        abort_latency, self.abort_latency = self.abort_latency, LatencyBucket()
        result["transactions"] = transactions.rollover()
        result["commit_latency"] = commit_latency.rollover()
        result["abort_latency"] = abort_latency.rollover()
        return result


metrics = TransactionalMetrics()

# --------------------------------------------------------------------------
# Shared statistics written by all producer worker threads, guarded by
# stats_lock (contention is low: once per committed record for the per-record
# histogram, once per transaction for the commit/abort histograms). Mirrors the
# C harness's stats_mutex and Java's HIST_LOCK.
# --------------------------------------------------------------------------
stats_lock = Lock()
completed_messages = 0   # committed records
verified = 0             # committed records that passed verification
committed_transactions = 0
aborted_transactions = 0
aborted_records = 0
total_latency_ms = 0
max_latency_ms = 0
total_commit_latency_ms = 0
max_commit_latency_ms = 0
total_abort_latency_ms = 0
max_abort_latency_ms = 0
latency_hist = [0] * (MAX_LATENCY_MS + 2)
commit_latency_hist = [0] * (MAX_LATENCY_MS + 2)
abort_latency_hist = [0] * (MAX_LATENCY_MS + 2)


def _hist_index(latency_ms):
    return min(max(int(latency_ms), 0), MAX_LATENCY_MS + 1)


def record_commit(commit_latency):
    """Record one committed transaction's commit latency (ms)."""
    global total_commit_latency_ms, max_commit_latency_ms, committed_transactions
    with stats_lock:
        metrics.transactions.add_measurement(1)
        metrics.commit_latency.add_measurement(commit_latency)
        committed_transactions += 1
        total_commit_latency_ms += commit_latency
        if commit_latency > max_commit_latency_ms:
            max_commit_latency_ms = commit_latency
        commit_latency_hist[_hist_index(commit_latency)] += 1


def record_abort(abort_latency, records):
    """Record one aborted transaction's abort latency (ms) plus its attempted
    record count. Deterministic-abort path only (symmetric with commit)."""
    global total_abort_latency_ms, max_abort_latency_ms
    global aborted_transactions, aborted_records
    with stats_lock:
        metrics.abort_latency.add_measurement(abort_latency)
        aborted_transactions += 1
        aborted_records += records
        total_abort_latency_ms += abort_latency
        if abort_latency > max_abort_latency_ms:
            max_abort_latency_ms = abort_latency
        abort_latency_hist[_hist_index(abort_latency)] += 1


def record_error_abort(records):
    """Count an error-driven abort (e.g. a failed commit/send_offsets): it is
    aborted but contributes NO abort-latency sample (matching the native
    harnesses, which only measure the deterministic-abort path)."""
    global aborted_transactions, aborted_records
    with stats_lock:
        aborted_transactions += 1
        aborted_records += records


def record_committed_record(latency, ok):
    """Record one committed record's per-record latency (ms) + throughput."""
    global completed_messages, verified, total_latency_ms, max_latency_ms
    with stats_lock:
        metrics.latency.add_measurement(latency)
        metrics.messages.add_measurement(1)
        metrics.bytes.add_measurement(message_size)
        completed_messages += 1
        if ok:
            verified += 1
        total_latency_ms += latency
        if latency > max_latency_ms:
            max_latency_ms = latency
        latency_hist[_hist_index(latency)] += 1


# Deterministic, evenly-spread abort selection (Bresenham). Per-producer 0-based
# transaction index i; abort iff floor((i+1)*rate) > floor(i*rate). Identical
# IEEE-754 arithmetic to the Rust/C/Java harnesses.
def should_abort(txn_index, rate):
    if rate <= 0.0:
        return False
    i = float(txn_index)
    return math.floor((i + 1.0) * rate) > math.floor(i * rate)


def now_ms():
    return int(time.time() * 1000)


# --------------------------------------------------------------------------
# Configuration builders
# --------------------------------------------------------------------------
def sasl_config_from_env(for_v2):
    """SASL config from the environment. ``for_v2`` selects the librdkafka form
    (username/password) vs. the Java/Rust form (jaas.config)."""
    security_protocol = os.environ.get("SECURITY_PROTOCOL", None)
    sasl_mechanism = os.environ.get("SASL_MECHANISM", None)
    sasl_username = os.environ.get("SASL_USERNAME", None)
    sasl_password = os.environ.get("SASL_PASSWORD", None)
    enabled = security_protocol in ("SASL_PLAINTEXT", "SASL_SSL") and \
        all([sasl_mechanism, sasl_username, sasl_password])
    if not enabled:
        return {}
    if for_v2:
        return {
            "security.protocol": security_protocol,
            "sasl.mechanism": sasl_mechanism,
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
        }
    jaas = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
        f"username=\"{sasl_username}\" \n\tpassword=\"{sasl_password}\";")
    return {
        "security.protocol": security_protocol,
        "sasl.mechanism": sasl_mechanism,
        "sasl.jaas.config": jaas,
    }


def producer_config(bootstrap_servers, txn_id):
    """Producer config for one transactional producer. Transactions force
    enable.idempotence=true and acks=all (the client rejects transactional.id
    otherwise)."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": f"{('python-librdkafka' if v2 else 'python-rust')}-txn-{txn_id}",
        "transactional.id": txn_id,
        "enable.idempotence": "true",
        "acks": "all",
    }
    conf.update(sasl_config_from_env(for_v2=v2))
    if not use_defaults:
        batch_size = 1024 * 1024
        if "BATCH_SIZE" in os.environ:
            batch_size = int(os.environ["BATCH_SIZE"]) * 1024
        if "MAX_REQUEST_SIZE" in os.environ:
            max_request_size = int(os.environ["MAX_REQUEST_SIZE"]) * 1024
        else:
            max_request_size = min(batch_size * 64, 8 * 1024 * 1024)
        compression = os.environ.get("COMPRESSION_TYPE", "none")
        linger = os.environ.get("LINGER_MS", "5")
        if v2:
            conf["batch.size"] = batch_size
            conf["message.max.bytes"] = max_request_size
            if "MAX_IN_FLIGHT" in os.environ:
                conf["max.in.flight.requests.per.connection"] = os.environ["MAX_IN_FLIGHT"]
        else:
            conf["batch.size"] = batch_size
            conf["max.request.size"] = max_request_size
            if "MAX_IN_FLIGHT" in os.environ:
                conf["max.in.flight.requests.per.connection"] = os.environ["MAX_IN_FLIGHT"]
        conf["compression.type"] = compression
        conf["linger.ms"] = linger
    return conf


def consumer_config(bootstrap_servers):
    """EOS consumer config. KIP-848 (group.protocol=consumer), auto-commit off
    (offsets flow through the transaction) and read_committed isolation. The
    python-rust binding is KIP-848-only; the librdkafka backend is set to
    KIP-848 too (Kaushik C:128) — needs a KIP-848-capable broker + client."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        # KIP-848 for BOTH backends (uniform across all harnesses).
        "group.protocol": "consumer",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "client.id": f"{('python-librdkafka' if v2 else 'python-rust')}-txn-consumer-{group_id}",
    }
    conf.update(sasl_config_from_env(for_v2=v2))
    if not v2:
        # max.poll.records is a Java/Rust consumer knob (not a librdkafka one);
        # the v2 backend bounds the batch via consume(num_messages=...) instead.
        conf["max.poll.records"] = str(records_per_transaction)
    return conf


def print_configuration(conf):
    print(f"Key size: {key_size} bytes")
    print(f"Value size: {value_size} bytes")
    print(f"Records per transaction: {records_per_transaction}")
    print(f"Abort rate: {abort_rate}")
    print(f"Transactional producers: {num_transactional_producers}")
    print(f"Transactional id base: {transactional_id}")
    print(f"TXN_MODE: {txn_mode}")
    if eos_mode:
        print(f"Source topic: {source_topic}")
        print(f"Consumer group id: {group_id}")
        if kafka_bin:
            print(f"Source seeding: kafka-producer-perf-test.sh from {kafka_bin} "
                  f"(throughput={seed_throughput}, num-records={seed_record_count()})")
        else:
            print("Source seeding: none (KAFKA_BIN unset; SOURCE_TOPIC assumed "
                  "pre-populated)")
    print(f"Verify: {do_verify}")
    print("enable.idempotence: true (forced for transactions)")
    print("Producer configuration:")
    for k, val in conf.items():
        if k in ("sasl.jaas.config", "sasl.password"):
            print(f"  {k}: <hidden>")
        else:
            print(f"  {k}: {val}")


# --------------------------------------------------------------------------
# Backend-neutral producer/consumer creation.
# --------------------------------------------------------------------------
def create_producer(bootstrap_servers, txn_id):
    conf = producer_config(bootstrap_servers, txn_id)
    if v2:
        from confluent_kafka import Producer as CKProducer
        return CKProducer(conf)
    from producer import KafkaProducer
    return KafkaProducer({k: str(val) for k, val in conf.items()})


def create_consumer(bootstrap_servers):
    conf = consumer_config(bootstrap_servers)
    if v2:
        from confluent_kafka import Consumer as CKConsumer
        c = CKConsumer(conf)
        c.subscribe([source_topic])
        return c
    from consumer import KafkaConsumer
    c = KafkaConsumer({k: str(val) for k, val in conf.items()})
    c.subscribe([source_topic])
    return c


def verify_v3(md):
    """Verify a v3 RecordMetadata (offset/partition non-negative, topic matches,
    timestamp present). Returns True on pass."""
    if not do_verify:
        return True
    try:
        return (md.offset() >= 0 and md.partition() >= 0
                and md.topic() == topic_name and md.timestamp() >= 0)
    except Exception:
        return False


def verify_v2(msg):
    """Verify a v2 delivered Message. Returns True on pass."""
    if not do_verify:
        return True
    try:
        _, ts = msg.timestamp()
        return (msg.offset() >= 0 and msg.partition() >= 0
                and msg.topic() == topic_name and ts > 0)
    except Exception:
        return False


# --------------------------------------------------------------------------
# Rate limiting: pace in 100 ms windows (per-producer rps / 10 records per
# checkpoint), matching the base harness and the native transactional tests.
# --------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, per_producer_rps, measured_start_ns):
        self.per_producer_rps = per_producer_rps
        self.checkpoint = max(per_producer_rps // 10, 1) if per_producer_rps else 0
        self.next_check_ns = measured_start_ns + 100_000_000

    def maybe_wait(self, records_sent):
        if not self.per_producer_rps or self.checkpoint == 0:
            return
        if records_sent % self.checkpoint == 0:
            now = time.time_ns()
            if now < self.next_check_ns:
                time.sleep((self.next_check_ns - now) / 1e9)
            self.next_check_ns += 100_000_000


def _time_bound_reached(measured_start_ns):
    return (time.time_ns() - measured_start_ns) > (test_duration_s + 1) * 1e9


# --------------------------------------------------------------------------
# Produce-mode worker (one per NUM_TRANSACTIONAL_PRODUCERS thread).
# --------------------------------------------------------------------------
def run_producer(index, bootstrap_servers, measured_start_ns,
                 per_producer_num_messages, per_producer_rps):
    txn_id = f"{transactional_id}-{index}"
    producer = create_producer(bootstrap_servers, txn_id)
    n = records_per_transaction
    rate = RateLimiter(per_producer_rps, measured_start_ns)
    txn_index = 0
    records_sent = 0
    msg_count = len(generated_messages)
    try:
        producer.init_transactions()
        cont = records_sent < per_producer_num_messages \
            if per_producer_num_messages > 0 else not terminating
        while cont:
            begin_ms = now_ms()
            producer.begin_transaction()

            produce_ms = []
            if v2:
                delivered = []  # list of (err, msg) appended by the DR callback
            else:
                futures = []
            for _ in range(n):
                key, value = generated_messages[records_sent % msg_count]
                produce_ms.append(now_ms())
                if v2:
                    _v2_produce(producer, topic_name, key, value, delivered)
                else:
                    from producer import ProducerRecord
                    futures.append(producer.send(
                        ProducerRecord(topic=topic_name, key=key, value=value)))
                records_sent += 1
                rate.maybe_wait(records_sent)

            if should_abort(txn_index, abort_rate):
                # Aborted transactions STILL produced their N records above; on
                # abort those records count toward neither throughput nor latency.
                producer.abort_transaction()
                abort_ms = now_ms()
                record_abort(abort_ms - begin_ms, n)
                if v2:
                    producer.poll(0)  # drain purge delivery reports
            else:
                producer.commit_transaction()
                commit_ms = now_ms()
                record_commit(commit_ms - begin_ms)
                results = _drain_committed(producer, delivered, n) if v2 else futures
                for r in range(n):
                    ok = _verify_committed(results, r)
                    record_committed_record(commit_ms - produce_ms[r], ok)

            txn_index += 1
            if per_producer_num_messages <= 0 and _time_bound_reached(measured_start_ns):
                break
            cont = not terminating
            if per_producer_num_messages > 0:
                cont = cont and records_sent < per_producer_num_messages
    except Exception as e:  # noqa: BLE001
        print(f"Producer {index} failed: {e}")
    finally:
        _close_producer(producer)


# --------------------------------------------------------------------------
# EOS-mode worker (read-process-write; one per NUM_TRANSACTIONAL_PRODUCERS).
# --------------------------------------------------------------------------
def run_eos_producer(index, bootstrap_servers, measured_start_ns,
                     per_producer_num_messages, per_producer_rps):
    txn_id = f"{transactional_id}-{index}"
    producer = create_producer(bootstrap_servers, txn_id)
    consumer = create_consumer(bootstrap_servers)
    rate = RateLimiter(per_producer_rps, measured_start_ns)
    txn_index = 0
    records_sent = 0
    consecutive_empty_polls = 0
    starvation_warned = False
    try:
        producer.init_transactions()
        cont = records_sent < per_producer_num_messages \
            if per_producer_num_messages > 0 else not terminating
        while cont:
            batch = _eos_poll(consumer)
            if not batch:
                # No source data this round: warn once if the source appears to be
                # starving the pipeline, then loop (bounded by the duration /
                # message target below) rather than opening an empty transaction.
                consecutive_empty_polls += 1
                if consecutive_empty_polls >= 10 and not starvation_warned:
                    print(
                        f"[WARN] EOS source starved: {consecutive_empty_polls} "
                        "consecutive empty polls from the source topic — the "
                        "source producer is not keeping up (the EOS throughput "
                        "ceiling is min(source-produce-rate, txn-process-rate)); "
                        "pre-populate/seed SOURCE_TOPIC with a high-throughput "
                        "producer", file=sys.stderr)
                    starvation_warned = True
                if per_producer_num_messages <= 0 and _time_bound_reached(measured_start_ns):
                    break
                cont = not terminating
                if per_producer_num_messages > 0:
                    cont = cont and records_sent < per_producer_num_messages
                continue
            consecutive_empty_polls = 0

            begin_ms = now_ms()
            producer.begin_transaction()
            produce_ms = []
            if v2:
                delivered = []
            else:
                futures = []
            offsets = {}  # (topic, partition) -> max consumed offset + 1
            for rec in batch:
                key, value, rtopic, rpart, roff = rec
                produce_ms.append(now_ms())
                if v2:
                    _v2_produce(producer, topic_name, key, value, delivered)
                else:
                    from producer import ProducerRecord
                    futures.append(producer.send(
                        ProducerRecord(topic=topic_name, key=key, value=value)))
                nxt = roff + 1
                cur = offsets.get((rtopic, rpart))
                if cur is None or nxt > cur:
                    offsets[(rtopic, rpart)] = nxt
                records_sent += 1
                rate.maybe_wait(records_sent)

            batch_records = len(batch)
            # v2 (librdkafka): flush queued produces before the txn control ops.
            # librdkafka's produce() is async (fire-and-forget); if the batch is
            # still un-flushed when abort_transaction() runs, those in-flight
            # ProduceRequests are purged (_PURGE_QUEUE) but the idempotent
            # base-sequence counter has already advanced, so the next txn's
            # produce to the same partition goes out ahead of the broker's
            # expected next-seq -> broker OUT_OF_ORDER_SEQUENCE_NUMBER -> the
            # transaction flips to a fatal AbortableError. Flushing persists the
            # batch first, so an abort has nothing to purge and the sequence
            # stays in step. (The rust/v3 path drains its accumulator before
            # control ops internally, so it never hits this.)
            if v2:
                producer.flush()
            # Send the consumed offsets to the transaction (offset + 1 per
            # partition, with the consumer group metadata).
            try:
                _send_offsets(producer, consumer, offsets)
            except Exception as e:  # noqa: BLE001
                print(f"send_offsets_to_transaction error: {e}")
                producer.abort_transaction()
                if v2:
                    producer.poll(0)
                record_error_abort(batch_records)
                txn_index += 1
                continue

            if should_abort(txn_index, abort_rate):
                producer.abort_transaction()
                abort_ms = now_ms()
                record_abort(abort_ms - begin_ms, batch_records)
                if v2:
                    producer.poll(0)
            else:
                producer.commit_transaction()
                commit_ms = now_ms()
                record_commit(commit_ms - begin_ms)
                results = _drain_committed(producer, delivered, batch_records) if v2 else futures
                for r in range(batch_records):
                    ok = _verify_committed(results, r)
                    record_committed_record(commit_ms - produce_ms[r], ok)

            txn_index += 1
            if per_producer_num_messages <= 0 and _time_bound_reached(measured_start_ns):
                break
            cont = not terminating
            if per_producer_num_messages > 0:
                cont = cont and records_sent < per_producer_num_messages
    except Exception as e:  # noqa: BLE001
        print(f"EOS producer {index} failed: {e}")
    finally:
        _close_producer(producer)
        _close_consumer(consumer)


# --------------------------------------------------------------------------
# Backend-specific helpers.
# --------------------------------------------------------------------------
def _v2_produce(producer, topic, key, value, delivered):
    """librdkafka produce with a per-record delivery callback that appends
    (err, msg) to ``delivered``. Retries on local queue overflow."""
    def cb(err, msg):
        delivered.append((err, msg))
    while not terminating:
        try:
            producer.produce(topic=topic, key=key, value=value, callback=cb)
            return
        except BufferError:
            producer.poll(0.001)


def _drain_committed(producer, delivered, n):
    """v2 only: after commit_transaction() (which flushes), serve delivery
    reports until all N have fired (or a short deadline). Returns the list of
    delivered (err, msg) tuples for verification."""
    deadline = time.time() + 30
    while len(delivered) < n and time.time() < deadline:
        producer.poll(0.1)
    return delivered


def _verify_committed(results, r):
    """Return True if record ``r`` in ``results`` verified. ``results`` is a list
    of concurrent.futures.Future (v3) or (err, msg) tuples (v2)."""
    try:
        if v2:
            if r >= len(results):
                return False
            err, msg = results[r]
            if err is not None:
                print(f"Delivery failed: {err}")
                return False
            return verify_v2(msg)
        return verify_v3(results[r].result())
    except Exception as e:  # noqa: BLE001
        print(f"Produce call resulted in exception: {e}")
        return False


def _eos_poll(consumer):
    """Poll the source for up to RECORDS_PER_TRANSACTION records, bounded by
    EOS_POLL_TIMEOUT_MS. Returns a list of (key, value, topic, partition,
    offset) tuples; an empty list means no source data this round."""
    out = []
    if v2:
        msgs = consumer.consume(num_messages=records_per_transaction,
                                timeout=EOS_POLL_TIMEOUT_MS / 1000.0)
        for msg in msgs:
            if msg is None or msg.error() is not None:
                continue
            out.append((msg.key(), msg.value(), msg.topic(),
                        msg.partition(), msg.offset()))
    else:
        records = consumer.poll(EOS_POLL_TIMEOUT_MS / 1000.0)
        if records is not None and not records.is_empty():
            for rec in records:
                key = bytes(rec.key) if rec.key is not None else None
                value = bytes(rec.value) if rec.value is not None else None
                out.append((key, value, rec.topic, rec.partition, rec.offset))
    return out


def _send_offsets(producer, consumer, offsets):
    """Send consumed offsets to the transaction, in the shape each backend
    expects."""
    if v2:
        from confluent_kafka import TopicPartition as CKTopicPartition
        offsets_list = [CKTopicPartition(t, p, off)
                        for (t, p), off in offsets.items()]
        producer.send_offsets_to_transaction(
            offsets_list, consumer.consumer_group_metadata())
    else:
        from consumer import TopicPartition, OffsetAndMetadata
        offset_map = {TopicPartition(t, p): OffsetAndMetadata(off)
                      for (t, p), off in offsets.items()}
        producer.send_offsets_to_transaction(offset_map, consumer.group_metadata())


def _close_producer(producer):
    try:
        if v2:
            producer.flush()
        else:
            producer.close()
    except Exception:  # noqa: BLE001
        pass


def _close_consumer(consumer):
    try:
        consumer.close()
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------
# Warmup — a few single-record transactions on one producer, to warm up
# connections. Mirrors the single-producer warmup of the native harnesses.
# --------------------------------------------------------------------------
def run_warmup(bootstrap_servers):
    print(f"Warming up for {warmup_s} seconds ...")
    producer = create_producer(bootstrap_servers, f"{transactional_id}-warmup")
    try:
        producer.init_transactions()
        warmup_end = time.time_ns() + warmup_s * 1_000_000_000
        i = 0
        msg_count = len(generated_messages)
        while time.time_ns() < warmup_end and not terminating:
            producer.begin_transaction()
            key, value = generated_messages[i % msg_count]
            if v2:
                delivered = []
                _v2_produce(producer, topic_name, key, value, delivered)
                producer.commit_transaction()
                _drain_committed(producer, delivered, 1)
            else:
                from producer import ProducerRecord
                fut = producer.send(ProducerRecord(topic=topic_name, key=key, value=value))
                producer.commit_transaction()
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"Warmup failed due to message verification error: {e}")
                    return
            time.sleep(0.1)
            i += 1
    except Exception as e:  # noqa: BLE001
        print(f"Warmup failed: {e}")
    finally:
        _close_producer(producer)
    print("Warmup complete.")


# --------------------------------------------------------------------------
# Summary + results.json
# --------------------------------------------------------------------------
def _summary_bucket(hist, total_ms, max_ms):
    total = sum(hist)
    min_ms = next((ms for ms, c in enumerate(hist) if c), 0)
    return {
        "min": min_ms,
        "avg": round(total_ms / total, 2) if total > 0 else 0.0,
        "p50": percentile_from_hist(hist, 0.50),
        "p90": percentile_from_hist(hist, 0.90),
        "p95": percentile_from_hist(hist, 0.95),
        "p99": percentile_from_hist(hist, 0.99),
        "p999": percentile_from_hist(hist, 0.999),
        "max": max_ms,
    }


def print_summary(measured_secs):
    latency_budget_exceeded = False
    ext = metrics.external_metrics_aggregations()
    avg_cpu = ext["average_cpu"] if ext["total_external_metrics"] > 0 else 0.0
    avg_rss_kib = (ext["average_rss"] / 1024.0) if ext["total_external_metrics"] > 0 else 0.0

    msg_rate = completed_messages / measured_secs if measured_secs > 0 else 0.0
    mib_rate = (completed_messages * message_size) / (1024.0 * 1024.0) / measured_secs \
        if measured_secs > 0 else 0.0
    txn_rate = committed_transactions / measured_secs if measured_secs > 0 else 0.0

    p99 = percentile_from_hist(latency_hist, 0.99)
    print()
    print(f"Duration: {measured_secs * 1000.0:.2f} ms")
    if ext["total_external_metrics"] > 0:
        print(f"Average CPU: {avg_cpu:.2f} %")
        print(f"Average RSS: {avg_rss_kib:.2f} KiB")
    else:
        print("No external metrics collected")
    print(f"Committed transactions: {committed_transactions}")
    print(f"Aborted transactions: {aborted_transactions}")
    print(f"Aborted records: {aborted_records}")
    print(f"Committed records: {completed_messages}")
    print(f"Average rate msg/s: {msg_rate:.2f} msg/s")
    print(f"Average rate MiB/s: {mib_rate:.2f} MiB/s")
    print(f"Transactions/s: {txn_rate:.2f}")
    print(f"p99 per-record latency: {p99} ms")
    print(f"p99 commit latency: {percentile_from_hist(commit_latency_hist, 0.99)} ms")
    print(f"p99 abort latency: {percentile_from_hist(abort_latency_hist, 0.99)} ms")

    client = ("python-librdkafka" if v2 else "python-rust") + "-txn"
    results = {
        "test": "transactional-producer",
        "client": client,
        "topic": topic_name,
        "messages_measured": completed_messages,
        "duration_s": round(measured_secs, 2),
        "throughput_msg_s": round(msg_rate, 2),
        "throughput_mib_s": round(mib_rate, 2),
        "committed_transactions": committed_transactions,
        "aborted_transactions": aborted_transactions,
        "aborted_records": aborted_records,
        "transactions_per_s": round(txn_rate, 2),
        "latency_ms": _summary_bucket(latency_hist, total_latency_ms, max_latency_ms),
        "commit_latency_ms": _summary_bucket(
            commit_latency_hist, total_commit_latency_ms, max_commit_latency_ms),
        "abort_latency_ms": _summary_bucket(
            abort_latency_hist, total_abort_latency_ms, max_abort_latency_ms),
        "cpu_avg_pct": round(avg_cpu, 2),
        "rss_avg_kib": round(avg_rss_kib, 2),
    }
    try:
        with open(results_file, "w") as fh:
            json.dump(results, fh, indent=2)
        print(f"Results summary written to: {results_file}")
    except OSError as e:
        print(f"Failed to write {results_file}: {e}")

    if p99_limit_ms > 0 and p99 > p99_limit_ms:
        latency_budget_exceeded = True
        print(f"p99 per-record latency {p99} ms exceeds budget {p99_limit_ms} ms")
    return latency_budget_exceeded


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def seed_record_count():
    """Number of records to seed into SOURCE_TOPIC via kafka-producer-perf-test.sh.

    Sized to outlast the whole measured window so the EOS consumer is never
    starved: an explicit SEED_COUNT wins; else NUM_MESSAGES when set; else
    ``sizing_rate x (duration + 30)``, where sizing_rate is SEED_THROUGHPUT when
    positive, otherwise a generous default. Mirrors the Rust harness's
    ``seed_record_count``.
    """
    if seed_count_env is not None:
        return max(1, int(seed_count_env))
    if num_messages > 0:
        return num_messages
    sizing_rate = seed_throughput if seed_throughput > 0 else 125_000
    return max(1, sizing_rate * (test_duration_s + 30))


def spawn_seed_producer(bootstrap_servers, total_records):
    """Seed SOURCE_TOPIC by spawning Kafka's standard kafka-producer-perf-test.sh
    (a plain Java producer) from ``$KAFKA_BIN`` — the SAME mechanism the consumer
    perf tests use to generate their input load (consumer_performance_test.py
    ``spawn_producer``, consumer-perf/src/main.rs). Canonical, cross-language
    EOS seeding: fill the source at high throughput before the measured interval.

    Seed-then-run: waits for the child to finish so the source is fully populated
    before the measured clock starts. SASL/security goes through a Java
    ``--producer.config`` properties file (its jaas value contains multiple '=',
    which ``--producer-props`` cannot carry). Returns True on success, False on a
    spawn/exit failure (surfaced as a non-fatal warning; the run then relies on
    whatever data SOURCE_TOPIC already holds).
    """
    bin_path = os.path.join(kafka_bin, "kafka-producer-perf-test.sh")
    record_size = max(1, message_size)
    # bootstrap + acks + SASL through a .properties file, exactly as
    # consumer_performance_test.py's spawn_producer does. The spawned tool is
    # Kafka's Java kafka-producer-perf-test.sh REGARDLESS of this harness's own
    # backend, so it needs the Java jaas form (for_v2=False) — the librdkafka
    # form (sasl.username/password) would fail to authenticate. Matches
    # consumer_performance_test.py:404 (v2=False).
    props = {"bootstrap.servers": bootstrap_servers, "acks": "1"}
    props.update(sasl_config_from_env(for_v2=False))
    fd, props_path = tempfile.mkstemp(prefix="txn_seed_", suffix=".properties")
    with os.fdopen(fd, "w") as f:
        for k, v in props.items():
            # A Java .properties value must be one logical line (a raw newline
            # ends the entry); collapse whitespace runs like the consumer harness.
            one_line = " ".join(str(v).split())
            f.write(f"{k}={one_line}\n")
    cmd = [
        bin_path,
        "--topic", source_topic,
        "--num-records", str(total_records),
        "--record-size", str(record_size),
        "--throughput", str(seed_throughput),
        "--producer.config", props_path,
    ]
    kind = "PEAK/unbounded" if seed_throughput < 0 else "fixed msg/s"
    print(f">>> Seeding source topic '{source_topic}' via {bin_path}: "
          f"throughput={seed_throughput} ({kind}), {record_size} bytes, "
          f"{total_records} records", flush=True)
    # Capture output so a silent seeder failure (which looks like "no data" on the
    # consumer side) stays inspectable, mirroring consumer_performance_test.py.
    try:
        with open("seed_producer.log", "w") as log:
            rc = subprocess.call(cmd, stdout=log, stderr=subprocess.STDOUT)
    except OSError as e:
        print(f"WARN: could not spawn kafka-producer-perf-test.sh from KAFKA_BIN "
              f"({e}); assuming SOURCE_TOPIC is pre-populated", file=sys.stderr)
        return False
    finally:
        try:
            os.remove(props_path)
        except OSError:
            pass
    if rc != 0:
        print(f"WARN: kafka-producer-perf-test.sh exited {rc}; see "
              f"seed_producer.log (assuming SOURCE_TOPIC has data)",
              file=sys.stderr)
        return False
    return True


# --------------------------------------------------------------------------
# Async twins (ASYNC=True): drive the async transaction API — v3
# AsyncKafkaProducer / v2 confluent_kafka.aio.AIOProducer — with awaited control
# ops and async sends. This exercises the async drain-before-control-ops path
# (each control op awaits the send tray drain before running). One asyncio task
# per NUM_TRANSACTIONAL_PRODUCERS; the EOS source poll uses the sync consumer
# (a blocking poll is fine for the single-producer scenarios and keeps the
# produce + control-op path fully async).
# --------------------------------------------------------------------------
def create_async_producer(bootstrap_servers, txn_id):
    conf = producer_config(bootstrap_servers, txn_id)
    if v2:
        from confluent_kafka.aio.producer import AIOProducer
        return AIOProducer({k: str(val) for k, val in conf.items()})
    from producer import AsyncKafkaProducer
    return AsyncKafkaProducer({k: str(val) for k, val in conf.items()})


async def _async_send(producer, key, value):
    """Async produce; returns an awaitable delivery future (v2 AIOProducer.produce
    / v3 AsyncKafkaProducer.send)."""
    if v2:
        return await producer.produce(topic=topic_name, key=key, value=value)
    from producer import ProducerRecord
    return await producer.send(ProducerRecord(topic=topic_name, key=key, value=value))


async def _send_offsets_async(producer, consumer, offsets):
    if v2:
        from confluent_kafka import TopicPartition as CKTopicPartition
        offsets_list = [CKTopicPartition(t, p, off) for (t, p), off in offsets.items()]
        await producer.send_offsets_to_transaction(
            offsets_list, consumer.consumer_group_metadata())
    else:
        from consumer import TopicPartition, OffsetAndMetadata
        offset_map = {TopicPartition(t, p): OffsetAndMetadata(off)
                      for (t, p), off in offsets.items()}
        await producer.send_offsets_to_transaction(offset_map, consumer.group_metadata())


def _verify_async_result(res):
    """Verify one gathered produce result (RecordMetadata v3 / Message v2 /
    Exception from return_exceptions gather)."""
    try:
        if isinstance(res, BaseException):
            print(f"Produce call resulted in exception: {res}")
            return False
        return verify_v2(res) if v2 else verify_v3(res)
    except Exception as e:  # noqa: BLE001
        print(f"verify error: {e}")
        return False


async def _close_producer_async(producer):
    try:
        if v2:
            await producer.flush()
        await producer.close()
    except Exception:  # noqa: BLE001
        pass


async def run_producer_async(index, bootstrap_servers, measured_start_ns,
                             per_producer_num_messages, per_producer_rps):
    txn_id = f"{transactional_id}-{index}"
    producer = create_async_producer(bootstrap_servers, txn_id)
    n = records_per_transaction
    rate = RateLimiter(per_producer_rps, measured_start_ns)
    txn_index = 0
    records_sent = 0
    msg_count = len(generated_messages)
    try:
        await producer.init_transactions()
        cont = records_sent < per_producer_num_messages \
            if per_producer_num_messages > 0 else not terminating
        while cont:
            begin_ms = now_ms()
            await producer.begin_transaction()
            produce_ms = []
            futures = []
            for _ in range(n):
                key, value = generated_messages[records_sent % msg_count]
                produce_ms.append(now_ms())
                futures.append(await _async_send(producer, key, value))
                records_sent += 1
                rate.maybe_wait(records_sent)
            if should_abort(txn_index, abort_rate):
                await producer.abort_transaction()
                abort_ms = now_ms()
                record_abort(abort_ms - begin_ms, n)
            else:
                await producer.commit_transaction()
                commit_ms = now_ms()
                record_commit(commit_ms - begin_ms)
                results = await asyncio.gather(*futures, return_exceptions=True)
                for r in range(n):
                    ok = _verify_async_result(results[r]) if r < len(results) else False
                    record_committed_record(commit_ms - produce_ms[r], ok)
            txn_index += 1
            if per_producer_num_messages <= 0 and _time_bound_reached(measured_start_ns):
                break
            cont = not terminating
            if per_producer_num_messages > 0:
                cont = cont and records_sent < per_producer_num_messages
    except Exception as e:  # noqa: BLE001
        print(f"Async producer {index} failed: {e}")
    finally:
        await _close_producer_async(producer)


async def run_eos_producer_async(index, bootstrap_servers, measured_start_ns,
                                 per_producer_num_messages, per_producer_rps):
    txn_id = f"{transactional_id}-{index}"
    producer = create_async_producer(bootstrap_servers, txn_id)
    consumer = create_consumer(bootstrap_servers)
    rate = RateLimiter(per_producer_rps, measured_start_ns)
    txn_index = 0
    records_sent = 0
    consecutive_empty_polls = 0
    starvation_warned = False
    try:
        await producer.init_transactions()
        cont = records_sent < per_producer_num_messages \
            if per_producer_num_messages > 0 else not terminating
        while cont:
            batch = _eos_poll(consumer)
            if not batch:
                consecutive_empty_polls += 1
                if consecutive_empty_polls >= 10 and not starvation_warned:
                    print("[WARN] EOS source starved (async): consecutive empty "
                          "polls from the source topic — seed SOURCE_TOPIC.",
                          file=sys.stderr)
                    starvation_warned = True
                if per_producer_num_messages <= 0 and _time_bound_reached(measured_start_ns):
                    break
                cont = not terminating
                if per_producer_num_messages > 0:
                    cont = cont and records_sent < per_producer_num_messages
                await asyncio.sleep(0)
                continue
            consecutive_empty_polls = 0
            begin_ms = now_ms()
            await producer.begin_transaction()
            produce_ms = []
            futures = []
            offsets = {}
            for rec in batch:
                key, value, rtopic, rpart, roff = rec
                produce_ms.append(now_ms())
                futures.append(await _async_send(producer, key, value))
                nxt = roff + 1
                cur = offsets.get((rtopic, rpart))
                if cur is None or nxt > cur:
                    offsets[(rtopic, rpart)] = nxt
                records_sent += 1
                rate.maybe_wait(records_sent)
            batch_records = len(batch)
            try:
                await _send_offsets_async(producer, consumer, offsets)
            except Exception as e:  # noqa: BLE001
                print(f"send_offsets_to_transaction error (async): {e}")
                await producer.abort_transaction()
                record_error_abort(batch_records)
                txn_index += 1
                continue
            if should_abort(txn_index, abort_rate):
                await producer.abort_transaction()
                abort_ms = now_ms()
                record_abort(abort_ms - begin_ms, batch_records)
            else:
                await producer.commit_transaction()
                commit_ms = now_ms()
                record_commit(commit_ms - begin_ms)
                results = await asyncio.gather(*futures, return_exceptions=True)
                for r in range(batch_records):
                    ok = _verify_async_result(results[r]) if r < len(results) else False
                    record_committed_record(commit_ms - produce_ms[r], ok)
            txn_index += 1
            if per_producer_num_messages <= 0 and _time_bound_reached(measured_start_ns):
                break
            cont = not terminating
            if per_producer_num_messages > 0:
                cont = cont and records_sent < per_producer_num_messages
    except Exception as e:  # noqa: BLE001
        print(f"Async EOS producer {index} failed: {e}")
    finally:
        await _close_producer_async(producer)
        _close_consumer(consumer)


async def _amain(bootstrap_servers, measured_start_ns,
                 per_producer_num_messages, per_producer_rps):
    worker = run_eos_producer_async if eos_mode else run_producer_async
    await asyncio.gather(*[
        worker(k, bootstrap_servers, measured_start_ns,
               per_producer_num_messages, per_producer_rps)
        for k in range(num_transactional_producers)
    ])


def main():
    bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")

    # Two modes: `produce` and `eos`. `eos` requires a SOURCE_TOPIC; any other
    # TXN_MODE is rejected outright.
    if eos_mode:
        if not source_topic:
            print("TXN_MODE=eos requires SOURCE_TOPIC (the input topic to "
                  "consume from). Aborting.", file=sys.stderr)
            sys.exit(2)
    elif txn_mode != "produce":
        print(f"Unknown TXN_MODE '{txn_mode}' (expected 'produce' or 'eos'). "
              "Aborting.", file=sys.stderr)
        sys.exit(2)

    if limit_rps is None:
        if num_messages > 0:
            print(f"Producing {num_messages} messages at max rate")
        else:
            print(f"Producing messages at max rate for {test_duration_s} seconds")
    else:
        print(f"Producing {num_messages} messages at {limit_rps} msg/s (total)")

    metrics.start_collecting(interval_s=1)
    print(f"Running transactional producer performance test {version_str} "
          f"(TXN_MODE={txn_mode})...")
    print_configuration(producer_config(bootstrap_servers, f"{transactional_id}-0"))

    # Only the destination topic is (re)created here. The EOS source topic is
    # seeded below when KAFKA_BIN is set (canonical), or assumed pre-populated
    # otherwise (see the module header).
    if create_topic:
        recreate_topic(bootstrap_servers, topic_name,
                       sasl_config_from_env(for_v2=True), partitions)

    # Canonical EOS source-topic seeding: with KAFKA_BIN set, fill SOURCE_TOPIC
    # via Kafka's standard kafka-producer-perf-test.sh (a Java producer) before
    # the measured interval — the same mechanism the consumer perf tests use.
    # Seed-then-run so the source is fully populated before the clock starts.
    if eos_mode and kafka_bin:
        spawn_seed_producer(bootstrap_servers, seed_record_count())

    if warmup_s > 0:
        run_warmup(bootstrap_servers)

    # === MEASURED INTERVAL ===
    metrics.measurement_start_ms = now_ms()
    measured_start_ns = time.time_ns()
    print(f"Starting measured interval at {metrics.measurement_start_ms} ms: "
          f"{datetime.datetime.now(tz=datetime.timezone.utc)}")

    # Split the total rate / message target evenly across producers.
    per_producer_num_messages = num_messages // num_transactional_producers \
        if num_messages > 0 else 0
    per_producer_rps = max(limit_rps // num_transactional_producers, 1) \
        if limit_rps else 0

    if run_async:
        # Async transaction API: one asyncio task per producer (awaited control
        # ops + async sends). Exercises the async drain-before-control-ops path.
        asyncio.run(_amain(bootstrap_servers, measured_start_ns,
                           per_producer_num_messages, per_producer_rps))
    else:
        worker = run_eos_producer if eos_mode else run_producer
        threads = []
        for k in range(num_transactional_producers):
            t = Thread(target=worker,
                       args=(k, bootstrap_servers, measured_start_ns,
                             per_producer_num_messages, per_producer_rps),
                       name=f"txn-producer-{k}")
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    metrics.measurement_end_ms = now_ms()
    measured_secs = (time.time_ns() - measured_start_ns) / 1e9

    if verified != completed_messages and not terminating:
        print(f"Verified messages {verified} does not match committed messages "
              f"{completed_messages}")

    latency_budget_exceeded = print_summary(measured_secs)

    if not terminating:
        print("Performing garbage collection...")
        gc.collect()
    print("Waiting for final metrics collection...")
    if not terminating:
        time.sleep(POST_TEST_AWAIT_SECONDS)
    metrics.stop_collecting()
    print("Done")
    sys.exit(1 if latency_budget_exceeded else 0)


def signal_handler(sig, frame):
    global terminating
    os.write(sys.stdout.fileno(), b"Termination signal received, shutting down...\n")
    terminating = True


# --------------------------------------------------------------------------
# In-suite pytest smoke test (short, asserted, Docker broker). Mirrors
# producer_performance_test.py's test_producer_e2e_latency: re-invokes this
# script as a subprocess with a short window, asserting a clean run. Skips (via
# the fixture) when Docker/testcontainers is unavailable.
# --------------------------------------------------------------------------
def test_transactional_producer_e2e_latency(kafka_broker):
    import subprocess as _sp

    import conftest

    topic = "txn-producer-perf-smoke"
    # The conftest broker sets transaction.state.log.replication.factor=1, so a
    # single-node transactional producer works.
    conftest.create_topic(kafka_broker, topic, partitions=4)

    env = dict(os.environ)
    env.update({
        "BOOTSTRAP_SERVERS": kafka_broker.external_bootstrap,
        "TOPIC_NAME": topic,
        "CLIENT_VERSION": "3",
        "TXN_MODE": "produce",
        "WARMUP_SECONDS": "0",
        "TEST_DURATION_SECONDS": "10",
        "LIMIT_RPS": "100",
        "VALUE_SIZE": "2048",
        "RECORDS_PER_TRANSACTION": "10",
        # Per-record latency in produce mode is dominated by transaction-fill
        # time; leave the p99 budget disabled in-suite (0), like the Rust test.
        "P99_LIMIT_MS": os.getenv("P99_LIMIT_MS", "0"),
        "DO_VERIFY": "False",
        # The fixture already created the topic; skip the delete+recreate.
        "CREATE_TOPIC": "False",
    })

    proc = _sp.run(
        [sys.executable, os.path.abspath(__file__)],
        env=env, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=180, capture_output=True, text=True)
    print(proc.stdout)
    print(proc.stderr, file=sys.stderr)
    assert proc.returncode == 0, (
        f"transactional producer perf run failed (rc={proc.returncode}); "
        "see output above")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
