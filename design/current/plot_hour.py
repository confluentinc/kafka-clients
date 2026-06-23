#!/usr/bin/env python3
"""Plot the 1-hour Rust vs librdkafka peak-run time series for visual comparison.

Inputs (design/current/): hour_{rust,lib}.log (harness interval lines) and
hour_{rust,lib}_cpu.csv (external per-15s /proc CPU+RSS sampler). Produces
hour_comparison.png — 4 stacked panels (CPU, throughput, latency, RSS) over the hour.
"""
import re
import statistics as st
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = "design/current"
RUST = "#d1495b"  # red
LIB = "#2e6f95"   # blue

ILINE = re.compile(
    r"t=\s*([\d.]+)s msgs=\s*\d+ thr=\s*(\d+) msg/s\s+avg=\s*[\d.]+ "
    r"p50=(\d+) p99=(\d+) p999=(\d+) max=(\d+) ms"
)


def parse_log(path):
    t, thr, p50, p99, p999, mx = [], [], [], [], [], []
    for ln in open(path):
        m = ILINE.search(ln)
        if m:
            t.append(float(m.group(1)) / 60.0)  # minutes
            thr.append(int(m.group(2)) / 1000.0)  # k msg/s
            p50.append(int(m.group(3)))
            p99.append(int(m.group(4)))
            p999.append(int(m.group(5)))
            mx.append(int(m.group(6)))
    return dict(t=t, thr=thr, p50=p50, p99=p99, p999=p999, mx=mx)


def parse_cpu(path):
    cpu, rss, idx = [], [], []
    i = 0
    for ln in open(path):
        if ln.startswith("epoch") or not ln.strip():
            continue
        parts = ln.split(",")
        cpu.append(float(parts[1]))
        rss.append(float(parts[2]))
        idx.append(i * 15 / 60.0)  # minutes (15s sampler)
        i += 1
    return dict(t=idx, cpu=cpu, rss=rss)


r, l = parse_log(f"{HERE}/hour_rust.log"), parse_log(f"{HERE}/hour_lib.log")
rc, lc = parse_cpu(f"{HERE}/hour_rust_cpu.csv"), parse_cpu(f"{HERE}/hour_lib_cpu.csv")


def mean(xs):
    return st.mean(xs) if xs else 0


fig, ax = plt.subplots(4, 1, figsize=(13, 15), sharex=True)
fig.suptitle(
    "1-hour peak run — Rust vs librdkafka  (Confluent Cloud SASL_SSL, 200p, 2KB, "
    "~320 MB/s, big-batch 4MB fetch)",
    fontsize=13, fontweight="bold",
)

# Panel 1: CPU
ax[0].plot(rc["t"], rc["cpu"], color=RUST, lw=1.1, label=f"Rust  (mean {mean(rc['cpu']):.1f}%)")
ax[0].plot(lc["t"], lc["cpu"], color=LIB, lw=1.1, label=f"librdkafka  (mean {mean(lc['cpu']):.1f}%)")
ax[0].axhline(mean(rc["cpu"]), color=RUST, ls=":", lw=0.8, alpha=0.6)
ax[0].axhline(mean(lc["cpu"]), color=LIB, ls=":", lw=0.8, alpha=0.6)
ax[0].set_ylabel("CPU (% of 1 core)")
ax[0].set_title("CPU utilization", fontsize=11)
ax[0].set_ylim(0, max(max(rc["cpu"]), max(lc["cpu"])) * 1.25)
ax[0].legend(loc="upper right")
ax[0].grid(alpha=0.3)

# Panel 2: Throughput
ax[1].plot(r["t"], r["thr"], color=RUST, lw=1.0, label=f"Rust  (mean {mean(r['thr']):.0f}k)")
ax[1].plot(l["t"], l["thr"], color=LIB, lw=1.0, label=f"librdkafka  (mean {mean(l['thr']):.0f}k)")
ax[1].set_ylabel("Throughput (k msg/s)")
ax[1].set_title("Throughput", fontsize=11)
ax[1].legend(loc="lower right")
ax[1].grid(alpha=0.3)

# Panel 3: Latency p50/p99/p999
ax[2].plot(r["t"], r["p99"], color=RUST, lw=1.0, label=f"Rust p99 (mean {mean(r['p99']):.0f}ms)")
ax[2].plot(l["t"], l["p99"], color=LIB, lw=1.0, label=f"librdkafka p99 (mean {mean(l['p99']):.0f}ms)")
ax[2].plot(r["t"], r["p50"], color=RUST, lw=0.9, ls="--", alpha=0.7, label=f"Rust p50 (mean {mean(r['p50']):.0f}ms)")
ax[2].plot(l["t"], l["p50"], color=LIB, lw=0.9, ls="--", alpha=0.7, label=f"librdkafka p50 (mean {mean(l['p50']):.0f}ms)")
ax[2].set_ylabel("E2E latency (ms)")
ax[2].set_title("Latency — p50 (dashed) & p99 (solid)", fontsize=11)
ax[2].legend(loc="upper right", ncol=2, fontsize=8)
ax[2].grid(alpha=0.3)

# Panel 4: RSS
ax[3].plot(rc["t"], rc["rss"], color=RUST, lw=1.0, label=f"Rust  (mean {mean(rc['rss']):.0f} MB)")
ax[3].plot(lc["t"], lc["rss"], color=LIB, lw=1.0, label=f"librdkafka  (mean {mean(lc['rss']):.0f} MB)")
ax[3].set_ylabel("RSS (MB)")
ax[3].set_title("Process memory (RSS)", fontsize=11)
ax[3].set_xlabel("elapsed (minutes)")
ax[3].set_ylim(0, max(max(rc["rss"]), max(lc["rss"])) * 1.2)
ax[3].legend(loc="center right")
ax[3].grid(alpha=0.3)

plt.tight_layout(rect=[0, 0, 1, 0.985])
out = f"{HERE}/hour_comparison.png"
plt.savefig(out, dpi=120, bbox_inches="tight")
print(f"wrote {out}")

# Print a compact stats summary too.
print("\n            Rust            librdkafka")
for nm, rv, lv in [
    ("CPU %", rc["cpu"], lc["cpu"]),
    ("RSS MB", rc["rss"], lc["rss"]),
    ("thr k", r["thr"], l["thr"]),
    ("p50 ms", r["p50"], l["p50"]),
    ("p99 ms", r["p99"], l["p99"]),
    ("p999 ms", r["p999"], l["p999"]),
]:
    print(f"{nm:>8}  {mean(rv):7.1f} [{min(rv):.0f}-{max(rv):.0f}]   {mean(lv):7.1f} [{min(lv):.0f}-{max(lv):.0f}]")
