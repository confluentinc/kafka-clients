import math
import psutil
import time
import json
from threading import Thread


# Latency histogram of the producer and transactional harnesses, matching the
# C/Rust/Java producer perf tests: 1 ms buckets covering 0..MAX_LATENCY_MS,
# plus one overflow bucket (MAX_LATENCY_MS + 1, the top bucket).
MAX_LATENCY_MS = 10000
# Latency histogram of the consumer harness, matching rust/consumer-perf,
# librdkafka_e2e.c and JavaE2E.java: 1 ms buckets covering
# 0..CONSUMER_MAX_LATENCY_MS, a longer latency counted in the top bucket. Under
# saturation the e2e latency is backlog age and can far exceed 10 s.
CONSUMER_MAX_LATENCY_MS = 600_000


def recreate_topic(bootstrap_servers, topic, sasl_conf=None, partitions=-1):
    """Delete `topic` (ignoring "does not exist"), wait 10s, re-create it, wait
    10s — using confluent-kafka's AdminClient (librdkafka), for either
    CLIENT_VERSION. Replication factor and (unless `partitions` > 0) partition
    count use the broker default (`-1`).

    `sasl_conf` is the librdkafka-form SASL dict (security.protocol,
    sasl.mechanism, sasl.username, sasl.password) or None/empty for PLAINTEXT.
    Runs through :func:`librdkafka_helpers.call`, i.e. in the librdkafka
    baseline venv (see that module).
    """
    import librdkafka_helpers
    librdkafka_helpers.call("recreate_topic", bootstrap_servers=bootstrap_servers,
                            topic=topic, sasl_conf=sasl_conf or {}, partitions=partitions)


def latency_bucket(latency_ns):
    """The 1 ms histogram bucket of a latency in nanoseconds:
    ``floor(latency_ns / 1_000_000)``, clamped to ``0..MAX_LATENCY_MS + 1`` (the
    rule of the Rust and C perf tests)."""
    return min(max(latency_ns // 1_000_000, 0), MAX_LATENCY_MS + 1)


def percentile_from_hist(hist, p):
    """Return the smallest latency-ms bucket whose cumulative count reaches the
    p-th percentile (0 < p <= 1). Mirrors percentileFromHist in the C/Rust/Java
    perf tests. ``hist`` is either a dense list indexed by bucket or a sparse
    ``{bucket: count}`` dict (same result for the same counts)."""
    if isinstance(hist, dict):
        buckets = sorted(hist.items())
        total = sum(count for _, count in buckets)
    else:
        buckets = enumerate(hist)
        total = sum(hist)
    if total == 0:
        return 0
    target = p * total
    cumulative = 0
    ms = 0
    for ms, count in buckets:
        cumulative += count
        if cumulative >= target:
            return ms
    return ms


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
    report p50/p90/p99/p999 per window, matching the C/Rust/Java perf tests.

    A measurement is a latency in ms (an int, or a float carrying the exact
    sub-ms value): average/max come from the exact values (``Bucket``), the
    percentiles from its ``floor`` bucket, clamped to ``top_bucket``. The
    histogram is sparse (``{bucket: count}``), so a window costs memory only for
    the buckets it touched, even with the consumer's 600 000 ms ceiling."""

    def __init__(self, top_bucket=MAX_LATENCY_MS + 1):
        super().__init__()
        self.top_bucket = top_bucket
        self.hist = {}

    def add_measurement(self, measurement):
        super().add_measurement(measurement)
        idx = min(max(int(measurement), 0), self.top_bucket)
        self.hist[idx] = self.hist.get(idx, 0) + 1

    def rollover(self):
        ret = super().rollover()
        ret["p50"] = str(percentile_from_hist(self.hist, 0.50))
        ret["p90"] = str(percentile_from_hist(self.hist, 0.90))
        ret["p99"] = str(percentile_from_hist(self.hist, 0.99))
        ret["p999"] = str(percentile_from_hist(self.hist, 0.999))
        self.hist = {}
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
    def __init__(self, latency_top_bucket=MAX_LATENCY_MS + 1):
        # Top bucket of the per-window latency histogram: the producer
        # harnesses' overflow bucket by default, CONSUMER_MAX_LATENCY_MS for
        # the consumer harness.
        self._latency_top_bucket = latency_top_bucket
        self.rss = MemoryBucket()
        self.cpu = CPUBucket()
        self.latency = LatencyBucket(latency_top_bucket)
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
        latency, self.latency = self.latency, LatencyBucket(self._latency_top_bucket)
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
