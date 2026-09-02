import asyncio
import datetime
import json
import os
import sys
import time
import random
import signal
import gc
import uuid
from threading import Thread


from performance_common import Metrics, MAX_LATENCY_MS, percentile_from_hist, recreate_topic
from concurrent.futures import CancelledError, Future
from producer import (KafkaProducer, AsyncKafkaProducer, ProducerRecord,
                      RecordMetadata)
from confluent_kafka import (Producer as CKProducer, Message as CKMessage,
                             Consumer, TopicPartition)
from confluent_kafka.aio.producer import AIOProducer as CKAIOProducer
from partitioner import partition_for_key


def message_generator(topic, key_size=100, value_size=1024,
                      n=100000, randomness=0.5, limit_rps=None):
    if limit_rps is not None and limit_rps <= 0:
        raise ValueError("limit_rps must be positive")

    ret = []
    rand_bytes = int(value_size * randomness)
    constant_bytes = random.randbytes(value_size - rand_bytes)

    key_constant_bytes = None
    key_rand_bytes = 0
    if key_size > 0:
        key_rand_bytes = int(key_size * randomness)
        key_constant_bytes = random.randbytes(key_size - key_rand_bytes)

    for _ in range(n):
        key = None
        if key_size > 0:
            key = key_constant_bytes + random.randbytes(key_rand_bytes)
        value = constant_bytes + random.randbytes(rand_bytes)
        ret.append((key, value))

    return ret


terminating = False
key_size = 0
value_size = 2048
verified = 0
warmup_sent = 0
measured_sent = 0
baseline_end_offsets = None  # {partition: offset}; set by main() pre-produce, None = not captured
topic_name = os.getenv("TOPIC_NAME", "test-topic")
limit_rps = os.getenv("LIMIT_RPS", None)
verify_consumed = os.getenv("VERIFY_CONSUMED", "False") == "True"
if 'KEY_SIZE' in os.environ:
    key_size = int(os.environ['KEY_SIZE'])
if 'VALUE_SIZE' in os.environ:
    value_size = int(os.environ['VALUE_SIZE'])
# Computed AFTER the KEY_SIZE/VALUE_SIZE env overrides: message_size feeds the
# bytes-per-message metrics and the MiB/s summary, so computing it from the
# defaults inflated (e.g.) a VALUE_SIZE=1024 run's byte rate by 2x.
message_size = key_size + value_size
if limit_rps is not None:
    limit_rps = int(limit_rps)
    # LIMIT_RPS <= 0 means unbounded (max rate), matching the Rust/C/Java perf
    # tests which treat 0 as "no rate limit". Normalize to None so the
    # unbounded/time-based path is taken (message_generator requires a positive
    # limit, and num_messages must not be forced to 0).
    if limit_rps <= 0:
        limit_rps = None
v2 = os.getenv("CLIENT_VERSION", "3") == "2"
run_async = os.getenv("ASYNC", "False") == "True"
do_verify = os.getenv("DO_VERIFY", "True") == "True"
use_defaults = os.getenv("USE_DEFAULTS", "False") == "True"
# When True (default), delete + re-create the topic before the run (broker-
# default partitions unless PARTITIONS is set; broker-default RF). See
# performance_common.recreate_topic.
create_topic = os.getenv("CREATE_TOPIC", "True") == "True"
partitions = int(os.getenv("PARTITIONS", "-1"))
warmup_s = int(os.getenv("WARMUP_SECONDS", "120"))
test_duration_s = os.getenv("TEST_DURATION_SECONDS", None)
if test_duration_s is not None:
    test_duration_s = int(test_duration_s)
else:
    test_duration_s = 600

# p99 latency budget (ms); 0 disables the assertion. Matches C/Rust/Java.
p99_limit_ms = int(os.getenv("P99_LIMIT_MS", "0"))
# Machine-readable summary file, matching the other producer perf tests.
results_file = os.getenv("RESULTS_FILE", "results.json")
# Seconds to keep collecting metrics after the measured interval, so the
# cooldown is captured in metrics.jsonl (but excluded from the averages).
POST_TEST_AWAIT_SECONDS = 10

# Cumulative latency histogram over the measured interval (1 ms buckets, plus
# one overflow bucket), used for the final p50/p90/p99/p999 summary. Warmup
# sends are awaited inline and never reach the recorder, so they are excluded.
latency_hist = [0] * (MAX_LATENCY_MS + 2)
# Set True when P99_LIMIT_MS > 0 and the measured p99 exceeds it.
latency_budget_exceeded = False


def _is_queue_full(exc):
    """True if `exc` is a librdkafka / confluent-kafka QUEUE_FULL delivery error
    (local producer queue overflow), e.g.
    KafkaError{code=_QUEUE_FULL,val=-184,...}. Handles both the confluent-kafka
    (v2) and Rust-binding (v3) backends."""
    try:
        from confluent_kafka import KafkaError
        arg = exc.args[0] if getattr(exc, "args", None) else None
        if arg is not None and hasattr(arg, "code") and arg.code() == KafkaError._QUEUE_FULL:
            return True
    except Exception:
        pass
    s = str(exc).lower()
    return "queue_full" in s or "queue full" in s


def record_latency(latency_ms):
    idx = min(max(int(latency_ms), 0), MAX_LATENCY_MS + 1)
    latency_hist[idx] += 1

num_messages = 0
if 'NUM_MESSAGES' in os.environ:
    num_messages = int(os.environ['NUM_MESSAGES'])

version_str = "v2 (confluent-kafka)" if v2 else \
    "v3 (confluent-kafka-rust)"
generated_messages = message_generator(topic_name, key_size=key_size,
                        value_size=value_size, n=10000,
                        limit_rps=limit_rps)
if limit_rps is not None:
    num_messages = int(limit_rps * test_duration_s)  # Run for the specified duration
total_size = num_messages * message_size
total_size_mib = total_size / (1024 * 1024)
producer = None
metrics = Metrics()


class CompatibleProducer:
    def __init__(self, configuration):
        self._producer = CKProducer(configuration)
        self._closed = False
        # PERF_DEBUG_TIMING=1: sampled produce() timing + per-second poll-thread
        # accounting (poll() returns the number of served callbacks, so the
        # aggregate is the delivery-report drain rate).
        self._debug_timing = os.getenv("PERF_DEBUG_TIMING") == "1"
        self._send_seq = 0

        def poll_producer():
            served = 0
            polls = 0
            window_start = time.monotonic()
            while not self._closed:
                served += self._producer.poll(1.0)
                polls += 1
                if self._debug_timing:
                    now = time.monotonic()
                    if now - window_start >= 1.0:
                        print(f"[TIMING][PY] tag=v2.poll_window served={served} polls={polls} "
                              f"window_s={now - window_start:.3f}", file=sys.stderr)
                        served = 0
                        polls = 0
                        window_start = now
        self._thread = Thread(target=poll_producer)
        self._thread.start()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def send(self, record, on_delivery=None):
        fut = Future()

        def delivery_report(err, msg):
            # Fires on the single poll thread at the broker ack. Stamp latency
            # here via on_delivery — mirroring the Rust binding's
            # on_delivery(metadata, exception) — so both backends record at the
            # delivery callback, not in a recorder that reads the clock later.
            if err is not None:
                exc = Exception(err)
                fut.set_exception(exc)
                if on_delivery is not None:
                    on_delivery(None, exc)
            else:
                fut.set_result(msg)
                if on_delivery is not None:
                    on_delivery(msg, None)

        self._send_seq += 1
        sample = self._debug_timing and self._send_seq % 512 == 0
        t0 = time.perf_counter_ns() if sample else 0
        retries = 0
        while not terminating:
            try:
                self._producer.produce(
                    topic=record.topic,
                    key=record.key,
                    value=record.value,
                    callback=delivery_report
                )
                break
            except BufferError:
                retries += 1
                time.sleep(0.001)
        if sample:
            print(f"[TIMING][PY] tag=v2.produce seq={self._send_seq} "
                  f"dur_ns={time.perf_counter_ns() - t0} buffer_full_retries={retries}",
                  file=sys.stderr)
        return fut

    def flush(self):
        # Stop the poll thread first so delivery callbacks fire only on the
        # caller's thread during the flush — preserving the single-writer
        # invariant the recorder relies on — then drive all pending deliveries.
        if not self._closed:
            self._closed = True
            self._thread.join()
        self._producer.flush()

    def close(self):
        self._closed = True
        self._thread.join()
        self._producer = None


class AsyncCompatibleProducer:
    """Async confluent-kafka-python (librdkafka) producer, for async-vs-async.

    Wraps ``confluent_kafka.aio.producer.AIOProducer`` so it exposes the same
    shape async_main expects: an async ``send`` returning a delivery future,
    plus async context-manager / ``close``. Mirrors the GraalVM reference perf
    test's ``AsyncCompatibleProducer``.
    """

    def __init__(self, configuration):
        num_messages_conf = min(num_messages, 2147483647) \
            if num_messages > 0 else 2147483647
        total_size_conf = min(total_size, 2147483647) \
            if total_size > 0 else 2147483647
        self._producer = CKAIOProducer({
            'queue.buffering.max.messages': num_messages_conf,
            'queue.buffering.max.kbytes': total_size_conf,
            **configuration,
        }, buffer_timeout=0.01)
        self._closed = False

        async def poll_producer():
            while not self._closed:
                await self._producer.poll(1)
        self._polling_task = asyncio.create_task(poll_producer())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def send(self, record):
        # AIOProducer.produce is a coroutine that returns the delivery future;
        # async_main adds a done-callback to it that records latency at the ack.
        return await self._producer.produce(
            topic=record.topic,
            key=record.key,
            value=record.value,
        )

    async def flush(self):
        # AIOProducer exposes an async flush(); if a given version doesn't, the
        # caller's drain-wait still catches stragglers via the poll task.
        try:
            await self._producer.flush()
        except AttributeError:
            pass

    async def close(self):
        self._closed = True
        await self._producer.close()
        await self._polling_task
        self._producer = None


def verify_record_metadata(r):
    global verified
    if not do_verify:
        verified += 1
        return

    if not isinstance(r, RecordMetadata):
        raise RuntimeError("Unexpected produce call result type")
    if (r.offset() >= 0 and r.partition() >= 0 and
            r.topic() == topic_name and r.timestamp() >= 0):
        verified += 1


def verify_message(m):
    global verified
    if not do_verify:
        verified += 1
        return

    if not isinstance(m, CKMessage):
        raise RuntimeError("Unexpected produce call result type")
    _, timestamp = m.timestamp()
    if (m.offset() >= 0 and m.partition() >= 0 and
            m.topic() == topic_name and timestamp > 0):
        verified += 1


verification_function = verify_message if v2 else verify_record_metadata


def sasl_config_from_env(v2=False):
    SECURITY_PROTOCOL = os.environ.get("SECURITY_PROTOCOL", None)
    SASL_MECHANISM = os.environ.get("SASL_MECHANISM", None)
    SASL_USERNAME = os.environ.get("SASL_USERNAME", None)
    SASL_PASSWORD = os.environ.get("SASL_PASSWORD", None)

    sasl_enabled = SECURITY_PROTOCOL in ("SASL_PLAINTEXT", "SASL_SSL") and \
                   all([SASL_MECHANISM, SASL_USERNAME, SASL_PASSWORD])
    if not sasl_enabled:
        return {}
    
    if not v2:
        sasl_jaas_config = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
            f"username=\"{SASL_USERNAME}\" \n\tpassword=\"{SASL_PASSWORD}\";")
        return {
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.mechanism': SASL_MECHANISM,
            'sasl.jaas.config': sasl_jaas_config
        }
    else:
        return {
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.mechanism': SASL_MECHANISM,
            'sasl.username': SASL_USERNAME,
            'sasl.password': SASL_PASSWORD
        }

def configuration_from_env(common_default_configuration, v2=False):
    # Default 1024 KiB batch, matching the C/Rust/Java perf tests.
    batch_size = 1024 * 1024
    conf = dict(common_default_configuration)
    conf.update(sasl_config_from_env(v2=v2))
    if 'BOOTSTRAP_SERVERS' in os.environ:
        conf['bootstrap.servers'] = os.environ['BOOTSTRAP_SERVERS']

    if not use_defaults:
        # acks=all, hardcoded like the C and Java perf tests (the Rust test relies
        # on the same client default).
        conf['acks'] = 'all'

        if 'BATCH_SIZE' in os.environ:
            batch_size = int(os.environ['BATCH_SIZE']) * 1024  # Convert KB to bytes
        conf['batch.size'] = batch_size

        if 'MAX_REQUEST_SIZE' in os.environ:
            max_request_size = int(os.environ['MAX_REQUEST_SIZE']) * 1024  # Convert KB to bytes
        else:
            # batch.size * 64, capped at 8 MiB — matches C/Rust/Java.
            max_request_size = min(batch_size * 64, 8 * 1024 * 1024)

        if not v2:
            conf['max.request.size'] = max_request_size
        else:
            conf['message.max.bytes'] = max_request_size

        if 'COMPRESSION_TYPE' in os.environ:
            conf['compression.type'] = os.environ['COMPRESSION_TYPE']
        else:
            conf['compression.type'] = 'none'

        if 'ENABLE_IDEMPOTENCE' in os.environ:
            conf['enable.idempotence'] = os.environ['ENABLE_IDEMPOTENCE']
        else:
            conf['enable.idempotence'] = 'false'

        if 'MAX_IN_FLIGHT' in os.environ:
            conf['max.in.flight.requests.per.connection'] = os.environ['MAX_IN_FLIGHT']

        if 'BUFFER_MEMORY' in os.environ:
            buffer_memory = int(os.environ['BUFFER_MEMORY']) * 1024 * 1024  # Convert MB to bytes
            if not v2:
                conf['buffer.memory'] = buffer_memory
            else:
                conf['queue.buffering.max.kbytes'] = buffer_memory // 1024  # Convert bytes to KB
                conf['queue.buffering.max.messages'] = 2147483647

        if 'LINGER_MS' in os.environ:
            conf['linger.ms'] = os.environ['LINGER_MS']
        else:
            conf['linger.ms'] = '5'  # default linger, matching C/Rust/Java
    return conf


def print_configuration(conf):
    print(f"Key size: {key_size} bytes")
    print(f"Value size: {value_size} bytes")
    print(f"Verify: {do_verify}")
    print("Producer configuration:")
    for key, value in conf.items():
        if key in ['sasl.jaas.config', 'sasl.password']:
            print(f"  {key}: <hidden>")
        else:
            print(f"  {key}: {value}")

def _verifier_consumer_config(bootstrap_servers, group_id):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
        'session.timeout.ms': '10000',
        'check.crcs': 'true'
    }
    conf.update(sasl_config_from_env(v2=True))
    return conf


def get_topic_end_offsets(bootstrap_servers, topic):
    """Returns {partition_id: high_watermark} for `topic`.

    Captures pre-existing topic state before the test starts so the end-of-run
    consumer can resume from these offsets and only see messages produced in
    this run. Returns {} if the topic does not yet exist (treated as "all
    partitions start at 0"); raises only on transport/auth failures.
    """
    consumer = Consumer(_verifier_consumer_config(
        bootstrap_servers, f"perf-baseline-{uuid.uuid4()}"))
    try:
        md = consumer.list_topics(topic, timeout=10)
        topic_md = md.topics.get(topic)
        if topic_md is None or topic_md.error is not None or not topic_md.partitions:
            return {}
        partitions = sorted(topic_md.partitions.keys())
        end_offsets = {}
        for p in partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=10)
            end_offsets[p] = high
        return end_offsets
    finally:
        consumer.close()


def verify_consumed_messages(bootstrap_servers, topic, baseline, expected_count, has_keys):
    """Consume `topic` starting at `baseline` per-partition offsets, count
    messages, and (if has_keys) check every message landed in the partition
    murmur2 would have chosen.

    `baseline` is {partition_id: starting_offset} captured before this test ran;
    starting from those offsets means the consumer only sees messages produced
    in this test, so `expected_count` is just the produced total (no need to
    add pre-existing). Missing partitions are assumed to start at 0.

    Returns 0 on success, 1 on any verification failure.
    """
    consumer = Consumer(_verifier_consumer_config(
        bootstrap_servers, f"perf-verify-{uuid.uuid4()}"))
    consumed_count = 0
    mismatch_count = 0
    sample_mismatches = []
    try:
        md = consumer.list_topics(topic, timeout=10)
        if topic not in md.topics or md.topics[topic].error is not None:
            print(f"Verification: cannot read metadata for {topic}")
            return 1
        partitions = sorted(md.topics[topic].partitions.keys())
        num_partitions = len(partitions)
        if num_partitions == 0:
            print(f"Verification: topic {topic} has no partitions")
            return 1

        targets = {}
        assignment = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=10)
            # Start from the baseline; if log retention has trimmed past it
            # since baseline capture, fall back to current low watermark.
            start = max(low, baseline.get(p, 0))
            targets[p] = high
            assignment.append(TopicPartition(topic, p, start))
        consumer.assign(assignment)

        current = {tp.partition: tp.offset for tp in assignment}

        # Each partition can stop independently when its end watermark is hit.
        # Allow up to ~60s of empty polls in a row before giving up — the
        # measured run can take minutes, but post-flush the topic is fully
        # readable so empty polls really do mean "we're caught up or stuck".
        empty_budget_s = 60.0
        last_progress_ns = time.time_ns()
        progress_print_at = 0
        progress_print_step = max(10000, expected_count // 20 if expected_count else 10000)

        def caught_up():
            return all(current[p] >= targets[p] for p in partitions)

        while not caught_up():
            msgs = consumer.consume(num_messages=1000, timeout=2.0)
            if not msgs:
                if (time.time_ns() - last_progress_ns) / 1e9 > empty_budget_s:
                    break
                continue
            saw_data = False
            for msg in msgs:
                err = msg.error()
                if err is not None:
                    print(f"Verification consume error: {err}")
                    continue
                saw_data = True
                consumed_count += 1
                p = msg.partition()
                current[p] = max(current[p], msg.offset() + 1)
                if has_keys:
                    key = msg.key()
                    if key is None:
                        mismatch_count += 1
                        if len(sample_mismatches) < 5:
                            sample_mismatches.append(
                                f"partition={p} offset={msg.offset()} "
                                "key=None (expected non-null)")
                        continue
                    expected_p = partition_for_key(bytes(key), num_partitions)
                    if expected_p != p:
                        mismatch_count += 1
                        if len(sample_mismatches) < 5:
                            sample_mismatches.append(
                                f"partition={p} expected={expected_p} "
                                f"offset={msg.offset()}")
                if consumed_count >= progress_print_at:
                    print(f"Verification: consumed {consumed_count} messages so far",
                          end='\r')
                    progress_print_at = consumed_count + progress_print_step
            if saw_data:
                last_progress_ns = time.time_ns()
    finally:
        consumer.close()

    print()
    count_ok = (consumed_count == expected_count)
    partitions_ok = (not has_keys) or (mismatch_count == 0)

    print(f"Consumer verification: consumed={consumed_count} "
          f"expected={expected_count} "
          f"(count_ok={count_ok})")
    if has_keys:
        print(f"Partition verification: mismatches={mismatch_count}/{consumed_count} "
              f"(partitions_ok={partitions_ok})")
        if sample_mismatches:
            print("First mismatches:")
            for s in sample_mismatches:
                print(f"  {s}")
    else:
        print("Partition verification: skipped (no keys)")

    return 0 if (count_ok and partitions_ok) else 1


def v3_producer(common_default_configuration):
    conf = configuration_from_env(common_default_configuration, v2=False)
    print_configuration(conf)
    conf = {k: str(v) for k, v in conf.items()}
    return KafkaProducer(conf)

def _maybe_enable_lrk_stats(conf, label):
    # PERF_LRK_STATS=1: librdkafka statistics JSON every 10s (per-broker rtt /
    # int_latency / outbuf_latency, per-partition msgq) for stage debugging.
    if os.getenv("PERF_LRK_STATS") == "1":
        conf['statistics.interval.ms'] = 10000
        conf['stats_cb'] = lambda js: print(f"[STATS-PY-{label}] {js}", file=sys.stderr)
    return conf


def v2_producer(common_default_configuration):
    conf = configuration_from_env(common_default_configuration, v2=True)
    # Match Apache Kafka's default partitioner so end-of-run partition
    # verification is apples-to-apples vs the v3 (Java/Rust) client.
    # librdkafka defaults to consistent_random (CRC32-based), not murmur2.
    if not use_defaults and os.getenv("SKIP_PARTITIONER_OVERRIDE", "0") != "1":
        conf['partitioner'] = 'murmur2_random'
    print_configuration(conf)
    _maybe_enable_lrk_stats(conf, "sync")
    return CompatibleProducer(conf)


def v3_async_producer(common_default_configuration):
    conf = configuration_from_env(common_default_configuration, v2=False)
    print_configuration(conf)
    conf = {k: str(v) for k, v in conf.items()}
    return AsyncKafkaProducer(conf)


def v2_async_producer(common_default_configuration):
    conf = configuration_from_env(common_default_configuration, v2=True)
    print_configuration(conf)
    _maybe_enable_lrk_stats(conf, "async")
    return AsyncCompatibleProducer(conf)


def print_measurement_summary(completed_messages, total_latency_ms,
                              max_latency_ms, before_ms, after_ms, after_ns,
                              first_message_time):
    """Print the throughput / latency / CPU / RSS report.

    Shared by the sync ``main()`` and the async ``async_main()`` so the two
    paths emit an identical summary.
    """
    metrics.measurement_end_ms = after_ms
    total_time_ns = after_ns - first_message_time
    total_time_ms = total_time_ns / 1_000_000
    total_time_s = total_time_ms / 1_000
    message_rate = completed_messages / total_time_s if total_time_s > 0 else 0
    external_metrics_aggregations = metrics.external_metrics_aggregations()
    print(f"End time: {after_ms} ms")
    print(f"Duration: {total_time_ms} ms")
    if external_metrics_aggregations["total_external_metrics"] > 0:
        average_cpu = external_metrics_aggregations['average_cpu']
        average_rss = external_metrics_aggregations['average_rss'] / 1024
        print(
            "Average CPU: "
            f"{average_cpu:.2f} %")
        print(
            "Average RSS: "
            f"{average_rss :.2f}"
            " KiB")
        print(f"CPU Efficiency: {message_rate / (average_cpu if average_cpu > 0 else 1):.2f} msg/(s * 1% CPU)")  # noqa: E501
        print(f"Memory Efficiency: {message_rate / (average_rss if average_rss > 0 else 1):.2f} msg/(s * KB RSS)")  # noqa: E501
    else:
        print("No external metrics collected")

    print(
        "Average time: "
        f"{total_time_ms / completed_messages:.2f} ms")
    print(
        "Average rate msg/s: "
        f"{message_rate:.2f} msg/s")
    print(
        "Average rate MiB/s: "
        f"{(completed_messages * message_size) / (1024.0 * 1024.0) / total_time_s:.2f} MiB/s")  # noqa: E501
    print(
        "Average latency: "
        f"{total_latency_ms / completed_messages:.2f} ms")
    print("Max latency: "
          f"{max_latency_ms:.2f} ms")
    # Percentiles from the cumulative measured-interval histogram, matching the
    # C/Rust/Java perf tests.
    p50 = percentile_from_hist(latency_hist, 0.50)
    p90 = percentile_from_hist(latency_hist, 0.90)
    p95 = percentile_from_hist(latency_hist, 0.95)
    p99 = percentile_from_hist(latency_hist, 0.99)
    p999 = percentile_from_hist(latency_hist, 0.999)
    print(f"p50 latency: {p50} ms")
    print(f"p90 latency: {p90} ms")
    print(f"p99 latency: {p99} ms")
    print(f"p999 latency: {p999} ms")

    # Machine-readable summary, kept in sync with the other producer perf
    # tests (same file name, keys and latency_ms shape as the consumer perf
    # test's results.json; `client` identifies which implementation wrote it).
    min_latency_ms = next((ms for ms, c in enumerate(latency_hist) if c), 0)
    client = ("python-librdkafka" if v2 else "python-rust") + \
        ("-async" if run_async else "")
    results = {
        "test": "producer", "client": client, "topic": topic_name,
        "messages_measured": completed_messages,
        "duration_s": round(total_time_s, 2),
        "throughput_msg_s": round(message_rate, 2),
        "throughput_mib_s": round(
            (completed_messages * message_size) / (1024.0 * 1024.0) / total_time_s
            if total_time_s > 0 else 0.0, 2),
        "latency_ms": {
            "min": min_latency_ms,
            "avg": round(total_latency_ms / completed_messages, 2)
            if completed_messages > 0 else 0.0,
            "p50": p50, "p90": p90, "p95": p95, "p99": p99, "p999": p999,
            "max": round(max_latency_ms, 2),
        },
        "cpu_avg_pct": round(external_metrics_aggregations.get('average_cpu', 0.0), 2)
        if external_metrics_aggregations["total_external_metrics"] > 0 else 0.0,
        "rss_avg_kib": round(external_metrics_aggregations.get('average_rss', 0.0) / 1024, 2)
        if external_metrics_aggregations["total_external_metrics"] > 0 else 0.0,
    }
    try:
        with open(results_file, "w") as fh:
            json.dump(results, fh, indent=2)
        print(f"Results summary written to: {results_file}")
    except OSError as e:
        print(f"Failed to write {results_file}: {e}")

    if p99_limit_ms > 0 and p99 > p99_limit_ms:
        global latency_budget_exceeded
        latency_budget_exceeded = True
        print(f"p99 latency {p99} ms exceeds budget {p99_limit_ms} ms")

def main(v2=False):
    global producer, verified, warmup_sent, measured_sent, baseline_end_offsets
    total_latency_ms = 0
    max_latency_ms = 0
    completed_messages = 0
    before_ms = None
    first_message_time = None

    common_default_configuration = {
        "bootstrap.servers": "localhost:9092",
    }
    bootstrap_servers = os.environ.get(
        "BOOTSTRAP_SERVERS",
        common_default_configuration["bootstrap.servers"])

    # Optionally start from a clean topic (delete + re-create) before producing.
    # Admin uses librdkafka (v2-form) SASL config regardless of CLIENT_VERSION.
    if create_topic:
        recreate_topic(bootstrap_servers, topic_name,
                       sasl_config_from_env(v2=True), partitions)

    if verify_consumed:
        try:
            baseline_end_offsets = get_topic_end_offsets(bootstrap_servers, topic_name)
            total_pre_existing = sum(baseline_end_offsets.values())
            print(f"Baseline: topic {topic_name} has {total_pre_existing} "
                  f"pre-existing messages across {len(baseline_end_offsets)} "
                  "partitions; verifier will start from these offsets")
        except Exception as e:
            print(f"Baseline capture failed: {e}. Verification will be skipped.")
            baseline_end_offsets = None

    if not v2:
        producer = v3_producer(common_default_configuration)
    else:
        producer = v2_producer(common_default_configuration)

    def record_delivery(metadata, exception, start_time):
        # Both backends record here, in their native delivery callback (v3
        # on_delivery / v2 delivery_report). It runs on the producer's single
        # completion/poll thread — the single writer of the latency state — and
        # stamps the latency at the broker ack, so a slow reader can never
        # inflate it (matches the native rust/C perf apps).
        nonlocal max_latency_ms, total_latency_ms, completed_messages
        if exception is not None:
            print(f"Produce call resulted in exception: {exception}")
        else:
            try:
                verification_function(metadata)
            except Exception as e:
                print(f"Produce call resulted in exception: {e}")
        completed_messages += 1
        current_latency = int(time.time() * 1000) - start_time
        metrics.latency.add_measurement(current_latency)
        record_latency(current_latency)
        metrics.messages.add_measurement(1)
        metrics.bytes.add_measurement(message_size)
        max_latency_ms = max(max_latency_ms, current_latency)
        total_latency_ms += current_latency

    try:
        with producer:
            generated_messages_len = len(generated_messages)           
            if warmup_s > 0:
                print(f"Warming up for {warmup_s} seconds ...")
                warmup_end_time = time.time_ns() + warmup_s * 1000000000
                i = 0
                while time.time_ns() < warmup_end_time:
                    message = generated_messages[i % generated_messages_len]
                    try:
                        produce_call = producer.send(ProducerRecord(
                            topic=topic_name,
                            key=message[0],
                            value=message[1]
                        ))
                        r = produce_call.result()
                        verification_function(r)
                        warmup_sent += 1
                    except Exception:
                        print("Warmup failed due to message verification error")
                        producer = None
                        return
                    time.sleep(0.1)
                    i += 1

            verified = 0

            before_ms = int(time.time() * 1000)
            first_message_time = time.time_ns()
            # Pace in 100 ms windows (limit_rps/10 messages per checkpoint),
            # matching the C, Rust and Java harnesses. Whole-second pacing would
            # burst a full second's quota into the client and measure burst
            # queueing rather than steady-state latency.
            rate_checkpoint = max(limit_rps // 10, 1) if limit_rps else 0
            next_check_time = first_message_time + 100_000_000
            metrics.measurement_start_ms = before_ms
            print(f"Starting measured interval at {before_ms} ms: {datetime.datetime.now(tz=datetime.timezone.utc)}")  # noqa: E501
            messages_sent = 0
            if num_messages > 0:
                continue_sending = messages_sent < num_messages
            else:
                continue_sending = not terminating

            while continue_sending:
                try:
                    key, value = generated_messages[messages_sent % generated_messages_len]
                    next_message = ProducerRecord(
                        topic=topic_name,
                        key=key,
                        value=value)
                    start_time = int(time.time() * 1000)
                    # Both backends record latency in their native delivery
                    # callback (stamped at the broker ack), not in a recorder.
                    producer.send(
                        next_message,
                        on_delivery=(lambda md, exc, st=start_time:
                                     record_delivery(md, exc, st)))
                    messages_sent += 1
                    limit_rps_reached = limit_rps and messages_sent % rate_checkpoint == 0
                    if limit_rps_reached:
                        now = time.time_ns()
                        if now < next_check_time:
                            time_to_wait_s = (next_check_time - now) / 1e9
                            time.sleep(time_to_wait_s)
                        next_check_time = next_check_time + 100_000_000
                    if messages_sent % 10000 == 0:
                        duration = time.time_ns() - first_message_time
                        exceeded_seconds = num_messages > 0 and 10 or 1
                        if duration > (test_duration_s + exceeded_seconds) * 1e9:
                            print(f"Test duration reached, {duration / 1e9:.2f} seconds. Interrupting...\n")  # noqa: E501
                            break
                except RuntimeError:
                    pass
            
                continue_sending = not terminating 
                if num_messages > 0:
                    continue_sending = continue_sending and messages_sent < num_messages

            if not terminating:
                # Flush so the producer drives every pending delivery to
                # completion, then wait for the delivery callbacks (which record
                # latency/metrics) to drain. Same barrier for both backends.
                producer.flush()
                drain_deadline = time.time() + 30
                while completed_messages < messages_sent and time.time() < drain_deadline:
                    time.sleep(0.01)
            measured_sent = messages_sent
            after_ms = int(time.time() * 1000)
            after_ns = time.time_ns()

            if verified != completed_messages:
                if not terminating:
                    print(f"Verified messages {verified} "
                          "does not match completed messages "
                          f"{completed_messages}")
            elif num_messages > 0 and completed_messages != num_messages:
                if not terminating:
                    print(f"Completed messages {completed_messages} "
                          f"does not match produced messages {num_messages}")
            else:
                print_measurement_summary(
                    completed_messages, total_latency_ms, max_latency_ms,
                    before_ms, after_ms, after_ns, first_message_time)
    except CancelledError:
        print("Main cancelled")

    producer = None


async def async_main():
    """Async counterpart of ``main()``.

    Drives an async producer on a single asyncio event loop: a recorder task
    awaits each delivery future and a bounded ``asyncio.Queue`` applies
    backpressure. Supports both async clients — the v3 Rust
    ``AsyncKafkaProducer`` and the v2 ``AsyncCompatibleProducer`` wrapping
    CKPy's ``AIOProducer`` — whose ``send`` coroutines both return the delivery
    future (``produce_call = await producer.send(record)``).
    """
    global producer, verified, warmup_sent, measured_sent, baseline_end_offsets
    total_latency_ms = 0
    max_latency_ms = 0
    completed_messages = 0
    # Messages whose delivery failed with QUEUE_FULL (local queue overflow):
    # they were never actually sent, so they are subtracted from measured_sent.
    queue_full = 0
    before_ms = None
    first_message_time = None

    common_default_configuration = {
        "bootstrap.servers": "localhost:9092",
    }
    bootstrap_servers = os.environ.get(
        "BOOTSTRAP_SERVERS",
        common_default_configuration["bootstrap.servers"])

    # Optionally start from a clean topic (delete + re-create) before producing.
    # Admin uses librdkafka (v2-form) SASL config regardless of CLIENT_VERSION.
    if create_topic:
        recreate_topic(bootstrap_servers, topic_name,
                       sasl_config_from_env(v2=True), partitions)

    if verify_consumed:
        try:
            baseline_end_offsets = get_topic_end_offsets(bootstrap_servers, topic_name)
            total_pre_existing = sum(baseline_end_offsets.values())
            print(f"Baseline: topic {topic_name} has {total_pre_existing} "
                  f"pre-existing messages across {len(baseline_end_offsets)} "
                  "partitions; verifier will start from these offsets")
        except Exception as e:
            print(f"Baseline capture failed: {e}. Verification will be skipped.")
            baseline_end_offsets = None

    producer = v2_async_producer(common_default_configuration) if v2 \
        else v3_async_producer(common_default_configuration)

    def record_delivery_async(fut, start_time):
        # Runs on the event loop when the delivery future resolves
        # (add_done_callback -> call_soon), stamping latency at completion so a
        # backed-up reader can't inflate it. Single-threaded (the loop), so no
        # lock is needed. Matches the sync path and the native rust/C apps.
        nonlocal max_latency_ms, total_latency_ms, completed_messages, queue_full
        # A cancelled future carries no result/exception (calling .exception()
        # on it would raise), so treat it like any other completion below.
        exc = None if fut.cancelled() else fut.exception()
        # QUEUE_FULL is the one outcome kept OUT of the throughput count: those
        # records were never sent, so they are subtracted from measured_sent
        # instead. Don't log it — it can occur tens of thousands of times.
        if exc is not None and _is_queue_full(exc):
            queue_full += 1
            return
        # Success, a non-QUEUE_FULL error, or a cancelled future all count toward
        # completed_messages (errors included), mirroring the sync path's
        # record_delivery. This also makes the drain converge — every message
        # bumps exactly one of completed_messages / queue_full — so a stray
        # error no longer leaves the drain waiting out its full 30s deadline.
        if exc is not None:
            print(f"Produce call resulted in exception: {exc}")
        elif not fut.cancelled():
            try:
                verification_function(fut.result())
            except Exception as e:
                print(f"Produce call resulted in exception: {e}")
        completed_messages += 1
        current_latency = int(time.time() * 1000) - start_time
        metrics.latency.add_measurement(current_latency)
        record_latency(current_latency)
        metrics.messages.add_measurement(1)
        metrics.bytes.add_measurement(message_size)
        max_latency_ms = max(max_latency_ms, current_latency)
        total_latency_ms += current_latency

    try:
        async with producer:
            generated_messages_len = len(generated_messages)
            if warmup_s > 0:
                print(f"Warming up for {warmup_s} seconds ...")
                warmup_end_time = time.time_ns() + warmup_s * 1000000000
                i = 0
                while time.time_ns() < warmup_end_time:
                    message = generated_messages[i % generated_messages_len]
                    try:
                        produce_call = await producer.send(ProducerRecord(
                            topic=topic_name,
                            key=message[0],
                            value=message[1]
                        ))
                        r = await produce_call
                        verification_function(r)
                        warmup_sent += 1
                    except Exception:
                        print("Warmup failed due to message verification error")
                        producer = None
                        return
                    await asyncio.sleep(0.1)
                    i += 1

            verified = 0

            before_ms = int(time.time() * 1000)
            first_message_time = time.time_ns()
            # Pace in 100 ms windows (limit_rps/10 messages per checkpoint),
            # matching the C, Rust and Java harnesses. Whole-second pacing would
            # burst a full second's quota into the client and measure burst
            # queueing rather than steady-state latency.
            rate_checkpoint = max(limit_rps // 10, 1) if limit_rps else 0
            next_check_time = first_message_time + 100_000_000
            metrics.measurement_start_ms = before_ms
            print(f"Starting measured interval at {before_ms} ms: {datetime.datetime.now(tz=datetime.timezone.utc)}")  # noqa: E501
            messages_sent = 0
            if num_messages > 0:
                continue_sending = messages_sent < num_messages
            else:
                continue_sending = not terminating

            while continue_sending:
                try:
                    key, value = generated_messages[messages_sent % generated_messages_len]
                    next_message = ProducerRecord(
                        topic=topic_name,
                        key=key,
                        value=value)
                    start_time = int(time.time() * 1000)
                    # Record latency in the delivery callback (stamped at the
                    # ack on the event loop), not in a recorder task.
                    fut = await producer.send(next_message)
                    fut.add_done_callback(
                        lambda f, st=start_time: record_delivery_async(f, st))
                    messages_sent += 1
                    limit_rps_reached = limit_rps and messages_sent % rate_checkpoint == 0
                    if limit_rps_reached:
                        now = time.time_ns()
                        if now < next_check_time:
                            await asyncio.sleep((next_check_time - now) / 1e9)
                        next_check_time = next_check_time + 100_000_000
                    if messages_sent % 10000 == 0:
                        # Yield to the in-flight completions and their delivery
                        # done-callbacks (which record latency/metrics).
                        await asyncio.sleep(0)
                        duration = time.time_ns() - first_message_time
                        exceeded_seconds = num_messages > 0 and 10 or 1
                        if duration > (test_duration_s + exceeded_seconds) * 1e9:
                            print(f"Test duration reached, {duration / 1e9:.2f} seconds. Interrupting...\n")  # noqa: E501
                            break
                except RuntimeError:
                    pass

                continue_sending = not terminating
                if num_messages > 0:
                    continue_sending = continue_sending and messages_sent < num_messages

            # Flush so the producer drives every pending delivery to completion,
            # then yield until the delivery callbacks (which record
            # latency/metrics) have all fired.
            await producer.flush()
            drain_deadline = time.time() + 30
            while (completed_messages + queue_full) < messages_sent \
                    and time.time() < drain_deadline:
                await asyncio.sleep(0.01)
            # QUEUE_FULL deliveries were never sent — subtract them from the
            # measured sent count.
            measured_sent = messages_sent - queue_full
            if queue_full > 0:
                print(f"QUEUE_FULL errors: {queue_full} (subtracted from sent; "
                      f"measured_sent={measured_sent})")
            after_ms = int(time.time() * 1000)
            after_ns = time.time_ns()

            if verified != completed_messages:
                if not terminating:
                    print(f"Verified messages {verified} "
                          "does not match completed messages "
                          f"{completed_messages}")
            elif num_messages > 0 and completed_messages != num_messages:
                if not terminating:
                    print(f"Completed messages {completed_messages} "
                          f"does not match produced messages {num_messages}")
            else:
                print_measurement_summary(
                    completed_messages, total_latency_ms, max_latency_ms,
                    before_ms, after_ms, after_ns, first_message_time)
    except CancelledError:
        print("Main cancelled")

    producer = None


def signal_handler(sig, frame):
    global terminating
    # Calling print inside a signal handler can lead to
    # a "reentrant call RuntimeError"
    os.write(sys.stdout.fileno(),
             b"Termination signal received, shutting down...\n")
    terminating = True

    try:
        # The async producer's close() is a coroutine; it cannot be driven from
        # a signal handler. Setting `terminating` breaks async_main's send loop,
        # and its `async with` block awaits close() on the way out.
        if producer and not run_async:
            producer.close()
    except Exception as e:
        os.write(sys.stdout.fileno(),
                 f"Exception during close: {e}".encode())


def test_producer_e2e_latency(kafka_broker):
    """Short producer-latency smoke run against a testcontainers Kafka broker.

    Re-invokes this script as a subprocess with a short measurement window and a
    p99 budget (its __main__ exits non-zero if the budget is exceeded), asserting
    a clean run. Skips (via the fixture) when Docker/testcontainers is
    unavailable. Mirrors the consumer perf test's in-suite entry."""
    import subprocess as _sp

    import conftest

    topic = "producer-perf-smoke"
    conftest.create_topic(kafka_broker, topic, partitions=4)

    # Match the Rust automatic perf test's in-suite config
    # (tests/integration/producer_perf_test.rs): 100 rps, 10 s, p99<=70 ms,
    # no warmup, default 2048-byte values.
    env = dict(os.environ)
    env.update({
        "BOOTSTRAP_SERVERS": kafka_broker.external_bootstrap,
        "TOPIC_NAME": topic,
        "CLIENT_VERSION": "3",
        "ASYNC": "False",
        "WARMUP_SECONDS": "0",
        "TEST_DURATION_SECONDS": "10",
        "LIMIT_RPS": "100",
        "VALUE_SIZE": "2048",
        # Inherit a looser P99_LIMIT_MS if set (e.g. the macOS run); 0 disables
        # the latency assert, default 70 keeps the Linux budget unchanged.
        "P99_LIMIT_MS": os.getenv("P99_LIMIT_MS", "70"),
        "DO_VERIFY": "False",
        # The fixture already created the topic via testcontainers; skip the
        # delete+recreate (and its 20s of sleeps) for the in-suite run.
        "CREATE_TOPIC": "False",
    })

    proc = _sp.run(
        [sys.executable, os.path.abspath(__file__)],
        env=env, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=180, capture_output=True, text=True)
    print(proc.stdout)
    print(proc.stderr, file=sys.stderr)
    assert proc.returncode == 0, (
        f"producer perf run failed (rc={proc.returncode}); see output above")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if not limit_rps:
        if num_messages > 0:
            print(f"Producing {num_messages} messages at max rate")
        else:
            print(f"Producing messages at max rate for {test_duration_s} seconds")
    else:
            print(f"Producing {num_messages} messages at "
                  f"{str(limit_rps)} msg/s")  # noqa: E501

    metrics.start_collecting(interval_s=1)
    if run_async:
        print(f"Running async producer performance test {version_str}...")
        asyncio.run(async_main())
    else:
        print(f"Running sync producer performance test {version_str}...")
        main(v2=v2)

    if not terminating:
        print("Performing garbage collection...")
        gc.collect()
    print("Waiting for final metrics collection...")
    # Keep collecting metrics through a short cooldown so the post-test window
    # is captured in metrics.jsonl (excluded from the averages, since the
    # measured interval has ended). Matches the C/Rust/Java perf tests.
    if not terminating:
        time.sleep(POST_TEST_AWAIT_SECONDS)
    last_metrics = metrics.external_metrics_last_values()
    print(f"Final CPU: {last_metrics['last_cpu']:.2f} %")
    print(f"Final RSS: {last_metrics['last_rss'] / 1024 :.2f} KiB")
    metrics.stop_collecting()
    print("Done")

    exit_code = 0
    if verify_consumed and not terminating and baseline_end_offsets is not None:
        bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
        expected = warmup_sent + measured_sent
        print(f"Verifying consumed messages from topic '{topic_name}' "
              f"starting at baseline offsets "
              f"(expected = {warmup_sent} warmup + {measured_sent} measured "
              f"= {expected})")
        try:
            exit_code = verify_consumed_messages(
                bootstrap_servers, topic_name,
                baseline_end_offsets, expected, key_size > 0)
        except Exception as e:
            print(f"Verification failed with exception: {e}")
            exit_code = 1
    elif not verify_consumed:
        print("Consumer verification skipped (VERIFY_CONSUMED=False)")
    elif terminating:
        print("Consumer verification skipped (terminated)")
    elif baseline_end_offsets is None:
        print("Consumer verification skipped (baseline capture failed)")

    # Fail the run if the p99 latency budget was exceeded (matches C/Rust/Java).
    if latency_budget_exceeded:
        exit_code = 1

    sys.exit(exit_code)
