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

"""librdkafka-backed helpers for the Python perf benchmarks.

Topic recreation and the producer benchmark's end-of-run verification use the
PyPI ``confluent-kafka`` client (librdkafka), for both backends, so the setup
and the verifier are independent of the client under test.

That package's top-level import name is ``confluent_kafka``, the same as this
repo's package, so the two cannot share one venv: installing both into one
``site-packages`` leaves neither importable. The librdkafka side therefore lives
in its own venv (``make init-venv-librdkafka``, ``venv-librdkafka/`` at the repo
root), where the ``CLIENT_VERSION=2`` baseline runs too.

:func:`call` runs a helper in-process when the running interpreter has the PyPI
package (the baseline venv) and otherwise re-runs this file under the baseline
venv's interpreter, named by ``LIBRDKAFKA_PYTHON`` (the Makefile sets it). The
helper's progress output goes to the caller's stdout / stderr; its result comes
back as JSON through a temporary file.
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid

LIBRDKAFKA_PYTHON_ENV = "LIBRDKAFKA_PYTHON"


def librdkafka_available():
    """True when ``confluent_kafka`` is the PyPI (librdkafka) package: only that
    package has the ``cimpl`` native module."""
    try:
        return importlib.util.find_spec("confluent_kafka.cimpl") is not None
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Helpers (run in the baseline venv)
# ---------------------------------------------------------------------------
def recreate_topic(bootstrap_servers, topic, sasl_conf=None, partitions=-1):
    """Delete `topic` (ignoring "does not exist"), wait 10s, re-create it, wait
    10s — using confluent-kafka's AdminClient (librdkafka). Replication factor
    and (unless `partitions` > 0) partition count use the broker default
    (`-1`), so this works on Confluent Cloud where RF=1 is rejected. The two
    sleeps let the delete/create metadata propagate across the cluster.

    `sasl_conf` is the librdkafka-form SASL dict (security.protocol,
    sasl.mechanism, sasl.username, sasl.password) or None/empty for PLAINTEXT.
    """
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaException, KafkaError

    admin = AdminClient({"bootstrap.servers": bootstrap_servers, **(sasl_conf or {})})

    print(f">>> CREATE_TOPIC: deleting topic '{topic}' (ignored if absent) ...", flush=True)
    for _t, fut in admin.delete_topics([topic], operation_timeout=30).items():
        try:
            fut.result()
            print(f">>> deleted '{_t}'", flush=True)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f">>> '{_t}' did not exist (ok)", flush=True)
            else:
                raise
    print(">>> waiting 10s after delete ...", flush=True)
    time.sleep(10)

    print(f">>> CREATE_TOPIC: creating topic '{topic}' "
          f"(partitions={'broker-default' if partitions < 0 else partitions}, "
          f"rf=broker-default) ...", flush=True)
    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=-1)
    for _t, fut in admin.create_topics([new_topic]).items():
        try:
            fut.result()
            print(f">>> created '{_t}'", flush=True)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f">>> '{_t}' already exists (ok)", flush=True)
            else:
                raise
    print(">>> waiting 10s after create ...", flush=True)
    time.sleep(10)


def _verifier_consumer_config(bootstrap_servers, group_id, sasl_conf):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
        'session.timeout.ms': '10000',
        'check.crcs': 'true'
    }
    conf.update(sasl_conf or {})
    return conf


def topic_end_offsets(bootstrap_servers, topic, sasl_conf=None):
    """Returns {partition_id: high_watermark} for `topic`.

    Captures pre-existing topic state before the test starts so the end-of-run
    consumer can resume from these offsets and only see messages produced in
    this run. Returns {} if the topic does not yet exist (treated as "all
    partitions start at 0"); raises only on transport/auth failures.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(_verifier_consumer_config(
        bootstrap_servers, f"perf-baseline-{uuid.uuid4()}", sasl_conf))
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


def verify_consumed_messages(bootstrap_servers, topic, baseline, expected_count, has_keys,
                             sasl_conf=None):
    """Consume `topic` starting at `baseline` per-partition offsets, count
    messages, and (if has_keys) check every message landed in the partition
    the default CRC-32 partitioner would have chosen (see partition_for_key).

    `baseline` is {partition_id: starting_offset} captured before this test ran;
    starting from those offsets means the consumer only sees messages produced
    in this test, so `expected_count` is just the produced total (no need to
    add pre-existing). Missing partitions are assumed to start at 0.

    Returns 0 on success, 1 on any verification failure.
    """
    from confluent_kafka import Consumer, TopicPartition
    from partitioner import partition_for_key

    consumer = Consumer(_verifier_consumer_config(
        bootstrap_servers, f"perf-verify-{uuid.uuid4()}", sasl_conf))
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


_HELPERS = {
    "recreate_topic": recreate_topic,
    "topic_end_offsets": topic_end_offsets,
    "verify_consumed_messages": verify_consumed_messages,
}


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
def _to_json(value):
    # {partition: offset} maps cross JSON as [[partition, offset], ...] so the
    # int keys survive the round trip.
    if isinstance(value, dict):
        return {"__pairs__": [[k, v] for k, v in value.items()]}
    return value


def _from_json(value):
    if isinstance(value, dict) and "__pairs__" in value:
        return {k: v for k, v in value["__pairs__"]}
    return value


def call(name, **kwargs):
    """Run helper `name` with `kwargs` and return its result — in-process in the
    baseline venv, otherwise under ``$LIBRDKAFKA_PYTHON``."""
    if librdkafka_available():
        return _HELPERS[name](**kwargs)
    python = os.environ.get(LIBRDKAFKA_PYTHON_ENV)
    if not python or not os.path.exists(python):
        raise RuntimeError(
            f"{name} needs the PyPI confluent-kafka (librdkafka) client, which lives "
            f"in its own venv: run `make init-venv-librdkafka` at the repo root and set "
            f"{LIBRDKAFKA_PYTHON_ENV} to its python (the perf Makefile targets do both); "
            f"got {LIBRDKAFKA_PYTHON_ENV}={python!r}")
    args = {k: _to_json(v) for k, v in kwargs.items()}
    # Keep the caller's buffered output ahead of the helper's.
    sys.stdout.flush()
    sys.stderr.flush()
    fd, result_path = tempfile.mkstemp(prefix="librdkafka_helper_", suffix=".json")
    os.close(fd)
    try:
        subprocess.run(
            [python, os.path.abspath(__file__), name, result_path],
            input=json.dumps(args), text=True, check=True)
        with open(result_path) as fh:
            return _from_json(json.load(fh))
    finally:
        os.unlink(result_path)


def _main(argv):
    name, result_path = argv[1], argv[2]
    kwargs = {k: _from_json(v) for k, v in json.load(sys.stdin).items()}
    result = _HELPERS[name](**kwargs)
    with open(result_path, "w") as fh:
        json.dump(_to_json(result), fh)
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
