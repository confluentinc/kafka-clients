# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Metric primitives for the soak client.

PROVENANCE: copied from `bindings/python/test/performance/performance_common.py`
and deliberately duplicated rather than imported.

The soak is a long-lived operational tool: a two-week run must not break because
a performance-test refactor changed a shared helper, and the soak's own needs
(append mode, a promptly-stoppable collector) must not distort code the perf
tests depend on. So the two files are independent by design.

**The JSONL record schema must stay identical to the perf harness's.** That
schema exists so soak and perf numbers are directly comparable across clients;
if you change the shape of a record here — the `rss`/`cpu`/`latency`/`bytes`/
`messages` blocks, the window timestamps, or the bucket fields — change
`performance_common.py` the same way, or say explicitly in the commit why the
two are diverging.

Differences from the original, all confined to `Metrics`:

* `__init__(path, mode)` — the original hardcodes `metrics.jsonl` opened `"w+"`.
  The soak needs a per-variant path, and append mode so `run.sh` restarting the
  client adds to the series instead of truncating it.
* `write_record()` / `close()` — writes are flushed, so a `kill -9` or a
  log-rotating supervisor cannot lose samples already collected.
* the collector thread waits on an `Event` instead of `time.sleep()`, so
  `stop_collecting()` returns immediately rather than waiting out the current
  window. At the soak's 10 s interval that was a 10 s stall on every shutdown.
"""

import math
import psutil
import time
import json
from threading import Event, Thread


# Latency histogram resolution, matching the C/Rust/Java perf tests: 1 ms
# buckets covering 0..MAX_LATENCY_MS, plus one overflow bucket.
MAX_LATENCY_MS = 10000


def recreate_topic(config, topic, partitions=-1):
    """Delete `topic` (ignoring "does not exist"), wait 10s, re-create it, wait
    10s — using this repo's Rust-backed AdminClient (bindings/python/admin.py).
    Replication factor and (unless `partitions` > 0) partition count use the
    broker default (`-1`), so this works on Confluent Cloud where RF=1 is
    rejected. The two sleeps let the delete/create metadata propagate across the
    cluster.

    `config` is the soak's Java-style admin config dict (bootstrap.servers,
    security.protocol, sasl.mechanism, sasl.jaas.config, ssl.*) — the same
    namespace the Rust producer/consumer use, so no credential translation is
    needed; the Rust admin client parses `sasl.jaas.config` itself.

    Each RPC returns one `concurrent.futures.Future` per key (`create_topics`
    -> `{name: Future}`, `delete_topics` -> `{name: Future}`): `fut.result()`
    blocks until it resolves, returning the value on success and raising a
    `KafkaError` (whose `.code` is the wire error code) on a per-key failure.

    The admin binding is imported lazily so this module stays importable (and
    unit-testable) without the Rust bindings, which build on Linux only.
    """
    from admin import AdminClient, NewTopic
    from producer import KafkaError
    # Reuse the startup fatal/transient classification so an ACL / auth failure
    # here surfaces as EXIT_FATAL rather than looping under the supervisor
    # (imported lazily to avoid a soakclient <-> soak_metrics import cycle).
    from soakclient import FatalStartupError, TransientStartupError

    # `Errors` wire codes (src/common/protocol/errors.rs); `.result()` raises a
    # `KafkaError` carrying one of these on a per-key failure.
    UNKNOWN_TOPIC_OR_PARTITION = 3
    TOPIC_ALREADY_EXISTS = 36
    # Authentication / authorization failures never clear by retrying; every
    # other error (broker unreachable, metadata timeout) might. Mirrors
    # `SoakClient._create_topic`.
    auth_codes = {
        29,   # TOPIC_AUTHORIZATION_FAILED
        30,   # GROUP_AUTHORIZATION_FAILED
        31,   # CLUSTER_AUTHORIZATION_FAILED
        33,   # UNSUPPORTED_SASL_MECHANISM
        34,   # ILLEGAL_SASL_STATE
        58,   # SASL_AUTHENTICATION_FAILED
    }

    def _classify(op, _topic, ex, benign_code):
        """Turn one topic's `KafkaError` (per-key value or whole-call) into an
        `ok` log, a `FatalStartupError`, or a `TransientStartupError`, exactly as
        `_create_topic` does. `benign_code` is the wire code that means "already
        in the desired state" for this op (topic absent on delete / present on
        create)."""
        code = ex.code
        if code == benign_code:
            benign = "did not exist" if op == "delete" else "already exists"
            print(f">>> '{_topic}' {benign} (ok)", flush=True)
        elif code in auth_codes:
            raise FatalStartupError(
                "authentication/authorization failed on {} of topic {!r}: {}. "
                "Check sasl.jaas.config (username/password) and the API key's "
                "ACLs. Restarting will not fix this.".format(op, _topic, ex.message)) from ex
        else:
            raise TransientStartupError(
                "could not {} topic {!r}: {}. If the cluster is reachable this "
                "may clear on retry.".format(op, _topic, ex.message)) from ex

    admin = AdminClient(dict(config))
    try:
        print(f">>> CREATE_TOPIC: deleting topic '{topic}' (ignored if absent) ...", flush=True)
        for _t, fut in admin.delete_topics([topic], timeout=30).items():
            try:
                fut.result()
                print(f">>> deleted '{_t}'", flush=True)
            except KafkaError as e:
                _classify("delete", _t, e, UNKNOWN_TOPIC_OR_PARTITION)
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
            except KafkaError as e:
                _classify("create", _t, e, TOPIC_ALREADY_EXISTS)
        print(">>> waiting 10s after create ...", flush=True)
        time.sleep(10)
    finally:
        admin.close()


def percentile_from_hist(hist, p):
    """Return the smallest latency-ms bucket whose cumulative count reaches the
    p-th percentile (0 < p <= 1). Mirrors percentileFromHist in the C/Rust/Java
    perf tests."""
    total = sum(hist)
    if total == 0:
        return 0
    target = p * total
    cumulative = 0
    for ms, count in enumerate(hist):
        cumulative += count
        if cumulative >= target:
            return ms
    return len(hist) - 1


class Bucket:
    def __init__(self):
        self.total = 0
        self.count = 0
        self.max = -math.inf

    def add_measurement(self, measurement):
        self.total += measurement
        self.count += 1
        if measurement > self.max:
            self.max = measurement

    def _average(self):
        if self.count == 0:
            return 0
        return self.total / self.count

    def _maximum(self):
        return self.max

    def _count(self):
        return self.count

    def rollover(self):
        total = self.total
        avg = self._average()
        mx = self._maximum()
        cnt = self._count()
        self.total = 0
        self.count = 0
        self.max = -math.inf
        return {
            "average": str(avg),
            "max": str(mx),
            "total": str(total),
            "count": str(cnt)
        }


class LatencyBucket(Bucket):
    """Bucket that additionally tracks a 1 ms-resolution histogram so it can
    report p50/p90/p99/p999 per window, matching the C/Rust/Java perf tests."""

    def __init__(self):
        super().__init__()
        self.hist = [0] * (MAX_LATENCY_MS + 2)

    def add_measurement(self, measurement):
        super().add_measurement(measurement)
        idx = min(max(int(measurement), 0), MAX_LATENCY_MS + 1)
        self.hist[idx] += 1

    def rollover(self):
        ret = super().rollover()
        ret["p50"] = str(percentile_from_hist(self.hist, 0.50))
        ret["p90"] = str(percentile_from_hist(self.hist, 0.90))
        ret["p99"] = str(percentile_from_hist(self.hist, 0.99))
        ret["p999"] = str(percentile_from_hist(self.hist, 0.999))
        self.hist = [0] * (MAX_LATENCY_MS + 2)
        return ret


class SingleMeasurementBucket(Bucket):
    def __init__(self):
        super().__init__()

    def add_single_measurement(self):
        raise NotImplementedError()

    def rollover(self):
        self.add_single_measurement()
        return super().rollover()


class MemoryBucket(SingleMeasurementBucket):

    def __init__(self):
        super().__init__()
        self.process = psutil.Process()

    def add_single_measurement(self):
        self.add_measurement(self.process.memory_info().rss)


class CPUBucket(SingleMeasurementBucket):

    def __init__(self):
        super().__init__()
        self.process = psutil.Process()

    def add_single_measurement(self):
        self.add_measurement(self.process.cpu_percent())


class Metrics:
    def __init__(self, path="metrics.jsonl", mode="w+"):
        """Collect rss/cpu/latency/bytes/messages and roll them over to a JSONL
        file.

        `path` and `mode` default to the perf harness's behaviour (truncate
        `metrics.jsonl` in the cwd). The soak passes a per-variant path and
        ``mode="a"`` so a restart appends instead of discarding the previous
        run's samples.
        """
        self.rss = MemoryBucket()
        self.cpu = CPUBucket()
        self.latency = LatencyBucket()
        self.bytes = Bucket()
        self.messages = Bucket()
        self.thread = None
        self.running = False
        self.total_external_metrics = 0
        self.total_cpu = 0
        self.total_rss = 0
        self.window_start_ms = int(time.time() * 1000)
        self.measurement_start_ms = -math.inf
        self.measurement_end_ms = -math.inf
        self.last_metrics = None
        self._stop = Event()
        self._fd = open(path, mode)

    def write_record(self, record):
        """Append one JSON object to the metrics file.

        Flushed on every write so a `kill -9` (or a log-rotating supervisor)
        cannot lose the samples already collected.
        """
        print(json.dumps(record), file=self._fd, flush=True)

    def close(self):
        """Close the metrics file. Safe to call more than once."""
        if self._fd is not None and not self._fd.closed:
            self._fd.close()

    def rollover(self):
        """Swap out the latency/bytes/messages buckets and return their totals.

        Not internally synchronized: the swap is a non-atomic read-then-replace
        on ``self.latency``/``self.bytes``/``self.messages``, so it is only
        safe if the caller serializes this against any concurrent writer of
        those attributes (e.g. an ``observe_message``-style method). This
        class has no lock of its own -- ``SoakMetrics`` in soakclient.py holds
        its own ``_lock`` around both `rollover()` and `observe_message()`
        for exactly this reason. A subclass or caller invoking this directly
        without equivalent serialization can lose or double-count samples.
        """
        window_start_ms, self.window_start_ms = \
            self.window_start_ms, int(time.time() * 1000)
        latency, self.latency = self.latency, LatencyBucket()
        bytes, self.bytes = self.bytes, Bucket()
        messages, self.messages = self.messages, Bucket()
        return {
            "rss": self.rss.rollover(),
            "cpu": self.cpu.rollover(),
            "latency": latency.rollover(),
            "bytes": bytes.rollover(),
            "messages": messages.rollover(),
            "window_start_ms": str(window_start_ms),
            "window_end_ms": str(self.window_start_ms),
            "measurement_start_ms": str(self.measurement_start_ms),
            "measurement_end_ms": str(self.measurement_end_ms)
        }

    def start_collecting(self, interval_s=1):
        if self.running:
            return
        self.running = True
        self._stop.clear()

        def collector():
            # Event.wait() rather than sleep(): stop_collecting() returns
            # promptly instead of waiting out the current interval, which for a
            # soak's 10 s window would otherwise stall every shutdown.
            while not self._stop.wait(interval_s):
                self.last_metrics = self.rollover()
                # Average CPU/RSS only over the measured interval — exclude
                # warmup (measurement not started) and post-test cooldown
                # (measurement ended) — matching the C/Rust/Java perf tests.
                in_measured = (self.measurement_start_ms != -math.inf
                               and self.measurement_end_ms == -math.inf)
                if in_measured:
                    self.total_external_metrics += 1
                    self.total_cpu += float(self.last_metrics["cpu"]["average"])
                    self.total_rss += float(self.last_metrics["rss"]["average"])
                self.write_record(self.last_metrics)

        # Daemon so a crash on the main thread (e.g. a failed recreate_topic at
        # startup) lets the process exit instead of hanging on this sampler.
        # Normal completion still joins it via stop_collecting().
        self.thread = Thread(target=collector, daemon=True)
        self.thread.start()

    def external_metrics_last_values(self):
        if not self.last_metrics:
            return {
                "last_cpu": 0.0,
                "last_rss": 0.0,
            }
        return {
            "last_cpu": float(self.last_metrics["cpu"]["average"]),
            "last_rss": float(self.last_metrics["rss"]["average"]),
        }

    def external_metrics_aggregations(self):
        count = self.total_external_metrics
        return {
            "total_external_metrics": count,
            "total_cpu": self.total_cpu,
            "total_rss": self.total_rss,
            "average_cpu": self.total_cpu / count if count > 0 else 0,
            "average_rss": self.total_rss / count if count > 0 else 0,
        }

    def stop_collecting(self):
        self.running = False
        self._stop.set()
        if self.thread:
            self.thread.join()
            self.thread = None
