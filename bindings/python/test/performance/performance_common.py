import math
import psutil
import time
import json
from threading import Thread


# Latency histogram resolution, matching the C/Rust/Java perf tests: 1 ms
# buckets covering 0..MAX_LATENCY_MS, plus one overflow bucket.
MAX_LATENCY_MS = 10000


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
    def __init__(self):
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
        self._fd = open("metrics.jsonl", "w+")

    def rollover(self):
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

        def collector():
            while self.running:
                time.sleep(interval_s)
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
                print(json.dumps(self.last_metrics), file=self._fd)

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
        return {
            "total_external_metrics": self.total_external_metrics,
            "total_cpu": self.total_cpu,
            "total_rss": self.total_rss,
            "average_cpu": self.total_cpu / self.total_external_metrics
                if self.total_external_metrics > 0 else 0,
            "average_rss": self.total_rss / self.total_external_metrics
                if self.total_external_metrics > 0 else 0,
        }

    def stop_collecting(self):
        self.running = False
        if self.thread:
            self.thread.join()
            self.thread = None
