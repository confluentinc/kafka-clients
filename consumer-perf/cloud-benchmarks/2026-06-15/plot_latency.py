import json, csv, glob
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = "/Users/shivsundarr/dev/njc-spike/example-confluent-kafka-rust-consumer/consumer-perf/cloud-benchmarks/2026-06-15"

def jsonl(pattern):
    p = glob.glob(pattern)[0]
    xs = []; p50 = []; p99 = []; p999 = []
    for line in open(p):
        if '"type":"interval"' in line:
            d = json.loads(line)
            xs.append(d["elapsed_s"] / 60.0)
            p50.append(d["lat_p50_ms"]); p99.append(d["lat_p99_ms"]); p999.append(d["lat_p999_ms"])
    return xs, p50, p99, p999

def javacsv(path):
    xs = []; p50 = []; p99 = []; p999 = []
    for row in csv.DictReader(open(path)):
        xs.append(float(row["t_s"]) / 60.0)
        p50.append(float(row["lat_p50"])); p99.append(float(row["lat_p99"])); p999.append(float(row["lat_p999"]))
    return xs, p50, p99, p999

workloads = [
    ("Workload 1 - tuned (fetch.min.bytes=4 MiB, batch 2000/2500, CRC off)",
     f"{BASE}/workload1_tuned_latency_over_time.png",
     {"librdkafka": jsonl(f"{BASE}/same-az/lr_repo/lr-repo-*/metrics.jsonl"),
      "rust":       jsonl(f"{BASE}/same-az/consumer-perf/results/lr-rust-*/metrics.jsonl"),
      "java":       javacsv(f"{BASE}/same-az/lr_java_intervals.csv")}),
    ("Workload 2 - default (fetch.min.bytes=1, max.poll.records=500, librdkafka single-poll)",
     f"{BASE}/workload2_default_latency_over_time.png",
     {"librdkafka": jsonl(f"{BASE}/same-az-default/ld_repo/ld-repo-*/metrics.jsonl"),
      "rust":       jsonl(f"{BASE}/same-az-default/consumer-perf/results/ld-rust-*/metrics.jsonl"),
      "java":       javacsv(f"{BASE}/same-az-default/ld_java_intervals.csv")}),
]
colors = {"librdkafka": "#1f77b4", "rust": "#d62728", "java": "#2ca02c"}

for title, out, data in workloads:
    fig, (ax50, ax99) = plt.subplots(2, 1, figsize=(11, 7.5), sharex=True)
    for cl in ["librdkafka", "rust", "java"]:
        xs, p50, p99, _ = data[cl]
        ax50.plot(xs, p50, label=cl, color=colors[cl], lw=1.6)
        ax99.plot(xs, p99, label=cl, color=colors[cl], lw=1.6)
    ax50.set_ylabel("p50 latency (ms)"); ax99.set_ylabel("p99 latency (ms)")
    ax99.set_xlabel("elapsed time (minutes)")
    ax50.set_title(title + "\nsame-AZ ARM (Graviton), 200p / 2 KB / ~300 MB/s, 30 min", fontsize=10)
    for ax in (ax50, ax99):
        ax.grid(alpha=0.3); ax.set_ylim(bottom=0); ax.legend(loc="upper right", fontsize=9)
    fig.tight_layout(); fig.savefig(out, dpi=120); print("wrote", out)
