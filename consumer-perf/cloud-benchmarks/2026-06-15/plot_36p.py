import json, csv, glob
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = "/Users/shivsundarr/dev/njc-spike/example-confluent-kafka-rust-consumer/consumer-perf/cloud-benchmarks/2026-06-15"
D = f"{BASE}/same-az-36p-default"

def jsonl(pattern):
    p = glob.glob(pattern)[0]
    xs, p50, p99 = [], [], []
    for line in open(p):
        if '"type":"interval"' in line:
            d = json.loads(line)
            xs.append(d["elapsed_s"] / 60.0); p50.append(d["lat_p50_ms"]); p99.append(d["lat_p99_ms"])
    return xs, p50, p99

def javacsv(path):
    xs, p50, p99 = [], [], []
    for row in csv.DictReader(open(path)):
        xs.append(float(row["t_s"]) / 60.0); p50.append(float(row["lat_p50"])); p99.append(float(row["lat_p99"]))
    return xs, p50, p99

data = {
    "librdkafka": jsonl(f"{D}/ld36_repo/ld-repo-*/metrics.jsonl"),
    "rust":       jsonl(f"{D}/rust-results/ld-rust-*/metrics.jsonl"),
    "java":       javacsv(f"{D}/ld36_java_intervals.csv"),
}
colors = {"librdkafka": "#1f77b4", "rust": "#d62728", "java": "#2ca02c"}
out = f"{BASE}/workload3_36p_default_latency_over_time.png"
fig, (ax50, ax99) = plt.subplots(2, 1, figsize=(11, 7.5), sharex=True)
for cl in ["librdkafka", "rust", "java"]:
    xs, p50, p99 = data[cl]
    ax50.plot(xs, p50, label=cl, color=colors[cl], lw=1.6)
    ax99.plot(xs, p99, label=cl, color=colors[cl], lw=1.6)
ax50.set_ylabel("p50 latency (ms)"); ax99.set_ylabel("p99 latency (ms)")
ax99.set_xlabel("elapsed time (minutes, measurement window after 4-min warmup)")
ax50.set_title("Workload 3 - default (fetch.min.bytes=1), 36 partitions\nsame-AZ ARM (Graviton), 2 KB / ~300 MB/s, 4-min warmup + 30-min measure", fontsize=10)
for ax in (ax50, ax99):
    ax.grid(alpha=0.3); ax.set_ylim(bottom=0); ax.legend(loc="upper right", fontsize=9)
fig.tight_layout(); fig.savefig(out, dpi=120); print("wrote", out)
