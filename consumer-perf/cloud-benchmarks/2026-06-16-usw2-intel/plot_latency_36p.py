#!/usr/bin/env python3
"""Latency-vs-time graph for the 36-partition latency-tuned (default) Intel USW2 run."""
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = "/tmp/lr36d_intervals.txt"
OUT = ("/Users/shivsundarr/dev/njc-spike/example-confluent-kafka-rust-consumer/"
       "consumer-perf/cloud-benchmarks/2026-06-16-usw2-intel/workload3_36p_default_latency_over_time.png")

SERIES = {"lr36d_rust": "Rust", "lr36d_repo": "librdkafka", "lr36d_java": "Java"}
COLOR = {"Rust": "#1f77b4", "librdkafka": "#ff7f0e", "Java": "#2ca02c"}
line_re = re.compile(r"t=\s*([\d.]+)s.*?p50=(\d+)\s+p99=(\d+)\s+p999=(\d+)")

data = {}
for raw in open(SRC):
    raw = raw.rstrip("\n")
    if "|" not in raw:
        continue
    series, line = raw.split("|", 1)
    if series not in SERIES:
        continue
    m = line_re.search(line)
    if not m:
        continue
    t, p50, p99, p999 = float(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    data.setdefault(SERIES[series], []).append((t/60.0, p50, p99, p999))

P99_YMAX = 320
fig, (ax50, ax99) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
clipped = []
for client in ("Rust", "librdkafka", "Java"):
    pts = sorted(data.get(client, []))
    if not pts:
        continue
    xs = [p[0] for p in pts]; p50s = [p[1] for p in pts]; p99s = [p[2] for p in pts]
    c = COLOR[client]
    ax50.plot(xs, p50s, color=c, lw=1.6, label=client)
    ax99.plot(xs, p99s, color=c, lw=1.6, marker=".", ms=4, label=client)
    for x, y in zip(xs, p99s):
        if y > P99_YMAX:
            clipped.append((client, x, y))
title = ("Intel USW2 - 36-PARTITION LATENCY-tuned (fetch.min=1, default partfetch, "
         "maxpoll=500 / librdkafka single-poll, ~300MB/s, 2KB, 4-min warmup)")
ax50.set_title(f"{title}\np50 (median) e2e latency over time", fontsize=10)
ax50.set_ylabel("p50 latency (ms)"); ax50.grid(True, alpha=0.3); ax50.legend(loc="upper right"); ax50.set_ylim(bottom=0)
ax99.set_title("p99 (tail) e2e latency over time", fontsize=11)
ax99.set_ylabel("p99 latency (ms)"); ax99.set_xlabel("time into 30-min measurement window (minutes)")
ax99.grid(True, alpha=0.3); ax99.legend(loc="upper right"); ax99.set_ylim(0, P99_YMAX)
if clipped:
    note = "off-scale: " + ", ".join(f"{cl} t={x:.1f}min p99={y}ms" for cl, x, y in clipped)
    ax99.annotate(note, xy=(0.5, 0.97), xycoords="axes fraction", ha="center", va="top",
                  fontsize=8, color="#888", style="italic")
fig.tight_layout(); fig.savefig(OUT, dpi=130)
print(f"wrote {OUT}")
for client in ("Rust", "librdkafka", "Java"):
    pts = sorted(data.get(client, []))
    if pts:
        p99s = [p[2] for p in pts]; p50s = [p[1] for p in pts]
        print(f"  {client:11s} n={len(pts)} p50med={sorted(p50s)[len(p50s)//2]} p99 min/med/max={min(p99s)}/{sorted(p99s)[len(p99s)//2]}/{max(p99s)}")
