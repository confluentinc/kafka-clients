#!/usr/bin/env python3
"""Latency-vs-time graphs for the two Intel (USW2, in-region) consumer workloads.
Source: per-interval lines parsed from the 30-min benchmark logs."""
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = "/tmp/latency_intervals.txt"
OUT_DIR = "/Users/shivsundarr/dev/njc-spike/example-confluent-kafka-rust-consumer/design/current"

SERIES = {
    "lr30_rust":  ("throughput", "Rust"),
    "lr30_repo":  ("throughput", "librdkafka"),
    "lr30_java":  ("throughput", "Java"),
    "lr30r_rust": ("latency",    "Rust"),
    "lr30d_repo": ("latency",    "librdkafka"),
    "lr30d_java": ("latency",    "Java"),
}
COLOR = {"Rust": "#1f77b4", "librdkafka": "#ff7f0e", "Java": "#2ca02c"}

line_re = re.compile(r"t=\s*([\d.]+)s.*?p50=(\d+)\s+p99=(\d+)\s+p999=(\d+)")

data = {"throughput": {}, "latency": {}}
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
    wl, client = SERIES[series]
    data[wl].setdefault(client, []).append((t/60.0, p50, p99, p999))

def make_fig(wl, title, fname, p99_ymax):
    fig, (ax50, ax99) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    clipped = []
    for client in ("Rust", "librdkafka", "Java"):
        pts = sorted(data[wl].get(client, []))
        if not pts:
            continue
        xs   = [p[0] for p in pts]
        p50s = [p[1] for p in pts]
        p99s = [p[2] for p in pts]
        c = COLOR[client]
        ax50.plot(xs, p50s, color=c, lw=1.6, label=client)
        ax99.plot(xs, p99s, color=c, lw=1.6, marker=".", ms=4, label=client)
        for x, y in zip(xs, p99s):
            if y > p99_ymax:
                clipped.append((client, x, y))
    ax50.set_title(f"{title}\np50 (median) e2e latency over time", fontsize=11)
    ax50.set_ylabel("p50 latency (ms)")
    ax50.grid(True, alpha=0.3); ax50.legend(loc="upper right"); ax50.set_ylim(bottom=0)
    ax99.set_title("p99 (tail) e2e latency over time", fontsize=11)
    ax99.set_ylabel("p99 latency (ms)"); ax99.set_xlabel("time into 30-min measurement window (minutes)")
    ax99.grid(True, alpha=0.3); ax99.legend(loc="upper right"); ax99.set_ylim(0, p99_ymax)
    if clipped:
        note = "off-scale (startup bootstrap-connect): " + ", ".join(
            f"{cl} t={x:.1f}min p99≈{y/1000:.0f}s" for cl, x, y in clipped)
        ax99.annotate(note, xy=(0.5, 0.97), xycoords="axes fraction", ha="center", va="top",
                      fontsize=8, color="#888", style="italic")
    fig.tight_layout()
    path = f"{OUT_DIR}/{fname}"
    fig.savefig(path, dpi=130)
    print(f"wrote {path}")
    for client in ("Rust", "librdkafka", "Java"):
        pts = sorted(data[wl].get(client, []))
        if pts:
            p99s = [p[2] for p in pts]; p50s = [p[1] for p in pts]
            print(f"  {client:11s} n={len(pts)}  p50 med={sorted(p50s)[len(p50s)//2]}  p99 min/med/max={min(p99s)}/{sorted(p99s)[len(p99s)//2]}/{max(p99s)}")

make_fig("throughput",
         "Intel USW2 - THROUGHPUT-tuned (fetch.min=4MiB, partfetch=4MiB, batch/maxpoll=2500/2000, ~300MB/s, 2KB)",
         "latency_over_time_throughput_tuned.png", p99_ymax=1000)
make_fig("latency",
         "Intel USW2 - LATENCY-tuned (fetch.min=1, default partfetch, maxpoll=500 / librdkafka single-poll, ~300MB/s, 2KB)",
         "latency_over_time_latency_tuned.png", p99_ymax=120)
