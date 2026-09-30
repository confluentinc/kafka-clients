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

"""End-to-end latency / CPU / memory benchmark for librdkafka
(confluent-kafka-python).

Mirrors the Rust `consumer-perf` harness methodology 1:1:

  1. Subscribe with auto.offset.reset=latest (group.protocol=consumer / KIP-848,
     fall back to classic if assignment never arrives) -> only new records seen.
  2. Wait until partitions are assigned (on_assign callback), settle to the live
     edge (poll until empty), THEN spawn kafka-producer-perf-test.sh at a fixed
     throughput.
  3. Poll loop; e2e latency per record = now_ms - msg.timestamp()[1].
     Skip `warmup` records, then measure `duration` seconds.
  4. Fixed-bucket streaming histogram (1ms buckets, 0..600000) for O(1)
     percentiles, no per-record allocation in the hot loop.
  5. Every `interval` seconds: sample CPU% (os.times() user+sys delta / wall
     delta * 100) and RSS (ru_maxrss), print an interval line, reset interval
     histogram, append a metrics.jsonl line.
  6. After `duration` s of measurement, stop the producer, print + persist a
     summary with the same JSONL schema as the Rust harness (client:"librdkafka").
"""

import argparse
import json
import os
import resource
import subprocess
import sys
import time

from confluent_kafka import Consumer, KafkaError


MAX_LATENCY_MS = 600_000  # match Rust harness ceiling


class LatencyHistogram:
    """Fixed-memory 1ms-bucket histogram, mirrors the Rust LatencyHistogram."""

    __slots__ = ("buckets", "count", "sum", "min", "max")

    def __init__(self):
        self.buckets = [0] * (MAX_LATENCY_MS + 1)
        self.count = 0
        self.sum = 0
        self.min = 1 << 62
        self.max = -(1 << 62)

    def record(self, latency_ms):
        idx = latency_ms
        if idx < 0:
            idx = 0
        elif idx > MAX_LATENCY_MS:
            idx = MAX_LATENCY_MS
        self.buckets[idx] += 1
        self.count += 1
        self.sum += latency_ms if latency_ms > 0 else 0
        if latency_ms < self.min:
            self.min = latency_ms
        if latency_ms > self.max:
            self.max = latency_ms

    def get_min(self):
        return 0 if self.count == 0 else self.min

    def get_max(self):
        return 0 if self.count == 0 else self.max

    def avg(self):
        return 0.0 if self.count == 0 else self.sum / self.count

    def percentile(self, pct):
        if self.count == 0:
            return 0
        import math
        target = math.ceil(pct / 100.0 * self.count)
        cumulative = 0
        for i, c in enumerate(self.buckets):
            cumulative += c
            if cumulative >= target:
                return i
        return MAX_LATENCY_MS


def now_millis():
    return int(time.time() * 1000)


class CpuSampler:
    """CPU% of one core (user+sys cpu-seconds delta / wall delta * 100) and RSS MB."""

    def __init__(self):
        t = os.times()
        self.last_cpu = t.user + t.system
        self.last_wall = time.monotonic()

    def sample(self):
        t = os.times()
        cpu = t.user + t.system
        wall = time.monotonic()
        dcpu = cpu - self.last_cpu
        dwall = wall - self.last_wall
        self.last_cpu = cpu
        self.last_wall = wall
        cpu_pct = (100.0 * dcpu / dwall) if dwall > 0 else 0.0
        # macOS: ru_maxrss is bytes; Linux: kilobytes. Detect by platform.
        ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            rss_mb = ru / (1024.0 * 1024.0)
        else:
            rss_mb = ru / 1024.0
        return cpu_pct, rss_mb


def spawn_producer(args, total_records):
    bin_path = os.path.join(args.kafka_bin, "kafka-producer-perf-test.sh")
    cmd = [
        bin_path,
        "--topic", args.topic,
        "--num-records", str(total_records),
        "--record-size", str(args.message_size),
        "--throughput", str(args.throughput),
        "--producer-props",
        f"bootstrap.servers={args.bootstrap}",
        "acks=1",
    ]
    print(f">>> Launching producer: throughput={args.throughput} fixed msg/s, "
          f"{args.message_size} bytes, ~{total_records} records")
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def build_consumer(args, protocol):
    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group_id,
        "client.id": "librdkafka-perf",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "fetch.min.bytes": 1,
    }
    if protocol == "consumer":
        conf["group.protocol"] = "consumer"
    else:
        conf["group.protocol"] = "classic"
    return Consumer(conf)


def run(args):
    print("=" * 70)
    print("Consumer E2E Latency Benchmark - librdkafka (confluent-kafka-python)")
    print("=" * 70)
    print(f"Bootstrap:   {args.bootstrap}")
    print(f"Topic:       {args.topic}")
    print(f"Group:       {args.group_id}")
    print(f"Throughput:  {args.throughput} msg/s")
    print(f"Duration:    {args.duration} s (after warmup)")
    print(f"Msg size:    {args.message_size} bytes")
    print(f"Warmup:      {args.warmup} messages")
    print(f"Interval:    {args.interval} s")
    print("=" * 70)

    assigned_flag = {"v": False}

    def on_assign(consumer, partitions):
        assigned_flag["v"] = True
        print(f"    assigned {len(partitions)} partition(s)")

    def on_revoke(consumer, partitions):
        pass

    protocol = args.protocol
    consumer = build_consumer(args, protocol)
    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
    print(f"\n>>> Subscribed; waiting for partition assignment "
          f"(group.protocol={protocol})...")

    poll_timeout = args.poll_timeout_ms / 1000.0
    join_start = time.monotonic()
    join_timeout = args.join_timeout
    last_log = time.monotonic()
    while True:
        msg = consumer.poll(poll_timeout)
        if assigned_flag["v"] and len(consumer.assignment()) > 0:
            print(f"    assigned after {time.monotonic() - join_start:.1f}s")
            break
        if time.monotonic() - last_log >= 5:
            print(f"    [join] {time.monotonic() - join_start:.0f}s elapsed, "
                  f"assignment={len(consumer.assignment())}")
            last_log = time.monotonic()
        if time.monotonic() - join_start >= join_timeout:
            print(f"ERROR: timed out ({join_timeout}s) waiting for assignment "
                  f"with group.protocol={protocol}", file=sys.stderr)
            consumer.close()
            return 2

    # Settle to the live edge before starting the producer (mirror Rust): poll
    # discarding records until two consecutive empty polls.
    print(">>> Settling to the live edge (polling until empty) before producer...")
    settle_deadline = time.monotonic() + 15
    empties = 0
    while True:
        msg = consumer.poll(poll_timeout)
        if msg is None:
            empties += 1
            if empties >= 2:
                break
        elif msg.error():
            empties += 1
        else:
            empties = 0
        if time.monotonic() >= settle_deadline:
            print("    (settle timeout - proceeding)")
            break
    print("    at live edge; starting producer now.")

    total_records = args.throughput * (args.duration + 30) + args.warmup
    producer = spawn_producer(args, total_records)

    overall = LatencyHistogram()
    interval_hist = LatencyHistogram()
    sampler = CpuSampler()

    # JSONL sink
    run_dir = os.path.join(args.results_dir, args.group_id)
    os.makedirs(run_dir, exist_ok=True)
    jsonl = open(os.path.join(run_dir, "metrics.jsonl"), "w")

    messages_consumed = 0
    warmup_complete = False
    measure_start = None
    interval_start = None
    interval_count = 0
    first_record_seen = False
    loop_start = time.monotonic()
    no_data_deadline = loop_start + 120

    print(f"\n>>> Measuring (warmup {args.warmup} msgs, then {args.duration} s)...\n")

    warmup = args.warmup
    duration = args.duration
    interval_s = args.interval

    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                if (not warmup_complete and time.monotonic() >= no_data_deadline
                        and messages_consumed == 0):
                    print("ERROR: no records within 120s (producer not running?)",
                          file=sys.stderr)
                    break
                if warmup_complete and (time.monotonic() - measure_start) >= duration:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue

            if not first_record_seen:
                first_record_seen = True
                print(f">>> first records arrived "
                      f"{time.monotonic() - loop_start:.1f}s after producer start")

            poll_now = now_millis()
            messages_consumed += 1
            if messages_consumed <= warmup:
                if messages_consumed == warmup:
                    warmup_complete = True
                    measure_start = time.monotonic()
                    interval_start = measure_start
                    print(f"    warmup complete ({warmup} msgs); measuring now")
                continue

            ts_type, ts = msg.timestamp()
            if ts > 0:
                latency = poll_now - ts
                overall.record(latency)
                interval_hist.record(latency)

            now_mono = time.monotonic()
            if warmup_complete and (now_mono - interval_start) >= interval_s:
                elapsed_s = now_mono - interval_start
                cpu, rss_mb = sampler.sample()
                icount = interval_hist.count
                throughput = icount / elapsed_s if elapsed_s > 0 else 0.0
                avg = interval_hist.avg()
                p50 = interval_hist.percentile(50)
                p99 = interval_hist.percentile(99)
                p999 = interval_hist.percentile(99.9)
                mx = interval_hist.get_max()
                total_elapsed = now_mono - measure_start
                print(f"[interval {interval_count}] t={total_elapsed:6.1f}s "
                      f"msgs={icount:>7} thr={throughput:>9.0f} msg/s  "
                      f"avg={avg:6.2f} p50={p50} p99={p99} p999={p999} max={mx} ms  "
                      f"cpu={cpu:5.1f}% rss={rss_mb:6.1f}MB")
                jsonl.write(json.dumps({
                    "type": "interval", "idx": interval_count,
                    "elapsed_s": round(total_elapsed, 2),
                    "interval_s": round(elapsed_s, 2),
                    "msgs": icount, "throughput_msg_s": round(throughput, 2),
                    "lat_avg_ms": round(avg, 2), "lat_p50_ms": p50,
                    "lat_p99_ms": p99, "lat_p999_ms": p999, "lat_max_ms": mx,
                    "cpu_pct": round(cpu, 1), "rss_mb": round(rss_mb, 1),
                }) + "\n")
                jsonl.flush()
                interval_hist = LatencyHistogram()
                interval_start = time.monotonic()
                interval_count += 1

            if warmup_complete and (time.monotonic() - measure_start) >= duration:
                break
    finally:
        measured_duration_s = (time.monotonic() - measure_start) if measure_start else 0.0
        try:
            producer.kill()
            producer.wait(timeout=5)
        except Exception:
            pass
        consumer.close()

    # Summary
    total_bytes = overall.count * args.message_size
    throughput_msg_s = (overall.count / measured_duration_s) if measured_duration_s > 0 else 0.0
    throughput_mib_s = ((total_bytes / (1024.0 * 1024.0)) / measured_duration_s
                        if measured_duration_s > 0 else 0.0)
    mn, avg, mx = overall.get_min(), overall.avg(), overall.get_max()
    p50 = overall.percentile(50)
    p90 = overall.percentile(90)
    p95 = overall.percentile(95)
    p99 = overall.percentile(99)
    p999 = overall.percentile(99.9)

    print("\n" + "=" * 70)
    print(f"SUMMARY - librdkafka (warmup {warmup} excluded, group.protocol={protocol})")
    print("=" * 70)
    print(f"Measured messages: {overall.count}")
    print(f"Duration:          {measured_duration_s:.2f} s")
    print(f"Throughput:        {throughput_msg_s:.0f} msg/s  ({throughput_mib_s:.2f} MiB/s)")
    print(f"E2E latency (ms):  min={mn} avg={avg:.2f} p50={p50} p90={p90} "
          f"p95={p95} p99={p99} p99.9={p999} max={mx}")
    print("=" * 70)

    jsonl.write(json.dumps({
        "type": "summary", "client": "librdkafka", "protocol": protocol,
        "messages": overall.count, "duration_s": round(measured_duration_s, 2),
        "throughput_msg_s": round(throughput_msg_s, 2),
        "throughput_mib_s": round(throughput_mib_s, 2),
        "lat_min_ms": mn, "lat_avg_ms": round(avg, 2), "lat_p50_ms": p50,
        "lat_p90_ms": p90, "lat_p95_ms": p95, "lat_p99_ms": p99,
        "lat_p999_ms": p999, "lat_max_ms": mx,
    }) + "\n")
    jsonl.flush()
    jsonl.close()
    print(f"\nResults written to: {run_dir}")
    return 0


def main():
    home = os.environ.get("HOME", ".")
    ts = now_millis()
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", "-b", default="localhost:9092")
    p.add_argument("--topic", "-t", default="consumer-perf-bench")
    p.add_argument("--group-id", "-g", default=f"cmp-librdkafka-{ts}")
    p.add_argument("--throughput", "-r", type=int, default=300000)
    p.add_argument("--duration", "-d", type=int, default=60)
    p.add_argument("--message-size", type=int, default=1024)
    p.add_argument("--partitions", type=int, default=12)
    p.add_argument("--warmup", "-w", type=int, default=5000)
    p.add_argument("--interval", type=int, default=5)
    p.add_argument("--poll-timeout-ms", type=int, default=500)
    p.add_argument("--join-timeout", type=int, default=120)
    p.add_argument("--protocol", default="consumer", choices=["consumer", "classic"])
    p.add_argument("--kafka-bin", default=f"{home}/dev/opensource/kafka/bin")
    p.add_argument("--results-dir", default="consumer-perf/results")
    args = p.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
