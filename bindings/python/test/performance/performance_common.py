import math
import psutil
import time
import json
from threading import Thread


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
        self.latency = Bucket()
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
        latency, self.latency = self.latency, Bucket()
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
                self.total_external_metrics += 1
                self.total_cpu += float(self.last_metrics["cpu"]["average"])
                self.total_rss += float(self.last_metrics["rss"]["average"])
                print(json.dumps(self.last_metrics), file=self._fd)

        self.thread = Thread(target=collector)
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
