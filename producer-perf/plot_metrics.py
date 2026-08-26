#!/usr/bin/env python3
"""
plot_metrics.py

Generates latency/throughput/RSS-over-time graphs from Kafka producer
perf-test metrics files: per-second window JSONL records, one JSON object
per line, with fields `latency.{average,p50,p90,p99,p999}`, `messages.count`,
`rss.average`, `cpu.average`, `window_start_ms`, `window_end_ms` (the schema
shared by the C/Java/Python harnesses' `metrics.jsonl` and the Rust
harness's own `<name>.jsonl`, e.g. `rust-native.jsonl`).

For every directory found under the given root(s) that contains such a file:

  1. Writes `graph.png` in that same directory: p50/p90/p99/avg latency,
     throughput, and RSS over the run's duration.

  2. Groups sibling case directories into "scenario" comparison groups
     (see `scenario_group_key`) and writes one `comparison.png` per group,
     in the group's common parent directory, overlaying every case's p50,
     p99, and throughput on one chart. A group needs 2+ cases to get a
     comparison chart; single-case dirs only get their own `graph.png`.

Rerun this after any future perf-test run by pointing it at the results
directory (or a whole results-archive root to refresh everything) --
it always regenerates from scratch, nothing needs cleaning first.

Usage:
    python3 plot_metrics.py <root_dir> [<root_dir> ...]

Requires matplotlib (`pip install matplotlib`).
"""
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

EXCLUDE_DIR_NAMES = {"remote-summaries"}
EXCLUDE_PATH_SUBSTRINGS = ["POLLUTED"]

CLIENT_COLORS = [
    "#e07b39", "#2f7d6b", "#5b6fd8", "#c2477a", "#8a8f27", "#3f9bc4", "#a35d1f",
    "#7a5195", "#ef5675", "#ffa600",
]

TARGET_BUCKET_POINTS = 120


def is_excluded(path: Path) -> bool:
    if path.name in EXCLUDE_DIR_NAMES:
        return True
    s = str(path)
    return any(sub in s for sub in EXCLUDE_PATH_SUBSTRINGS)


def find_metrics_file(case_dir: Path):
    """Prefer `metrics.jsonl`; else the first *.jsonl file in the directory
    whose first non-empty line matches the expected schema (handles the
    Rust harness's own `<name>.jsonl` naming)."""
    candidate = case_dir / "metrics.jsonl"
    if candidate.is_file():
        return candidate
    for j in sorted(case_dir.glob("*.jsonl")):
        try:
            with open(j) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    r = json.loads(line)
                    if "latency" in r and "messages" in r and "window_start_ms" in r:
                        return j
                    break
        except (json.JSONDecodeError, OSError):
            continue
    return None


def pick_bucket_seconds(path: Path) -> int:
    """Scale bucket width so a run of any length yields ~TARGET_BUCKET_POINTS
    points (the file is ~1 line/second regardless of throughput)."""
    n = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                n += 1
    if n <= 1:
        return 1
    return max(1, n // TARGET_BUCKET_POINTS)


def load_bucketed(path: Path, bucket_s: int):
    """Bucket into `bucket_s`-second bins, skipping warmup (messages.count==0)
    rows. Returns a list of dicts sorted by time, one per non-empty bucket."""
    buckets = defaultdict(lambda: {"p50": [], "p90": [], "p99": [], "avg": [], "msgs": 0.0, "rss": [], "n": 0})
    t0 = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                msg_count = float(r["messages"]["count"])
            except (KeyError, TypeError, ValueError):
                continue
            if msg_count <= 0:
                continue
            try:
                ws = int(r["window_start_ms"])
                we = int(r["window_end_ms"])
            except (KeyError, TypeError, ValueError):
                continue
            if t0 is None:
                t0 = ws
            bucket = (ws - t0) // (bucket_s * 1000)
            b = buckets[bucket]
            lat = r.get("latency", {})
            b["p50"].append(float(lat.get("p50", 0) or 0))
            b["p90"].append(float(lat.get("p90", 0) or 0))
            b["p99"].append(float(lat.get("p99", 0) or 0))
            b["avg"].append(float(lat.get("average", 0) or 0))
            b["msgs"] += msg_count
            try:
                b["rss"].append(float(r["rss"]["average"]) / (1024 * 1024))
            except (KeyError, TypeError, ValueError):
                pass
            b["n"] += 1

    out = []
    for bucket in sorted(buckets):
        b = buckets[bucket]
        if b["n"] == 0:
            continue
        out.append({
            "t_min": bucket * (bucket_s / 60.0),
            "p50": sum(b["p50"]) / b["n"],
            "p90": sum(b["p90"]) / b["n"],
            "p99": sum(b["p99"]) / b["n"],
            "avg": sum(b["avg"]) / b["n"],
            "thpt": b["msgs"] / bucket_s,
            "rss": (sum(b["rss"]) / len(b["rss"])) if b["rss"] else 0.0,
        })
    return out


def scenario_group_key(name: str):
    """Extract a grouping key so sibling case dirs that belong to the same
    scenario get one overlay comparison chart, without merging dirs that
    only share a parent by coincidence (e.g. a 3hr run and its own later
    10-minute follow-up run of the same client).

    Handles three naming conventions seen in this project's perf harnesses:
      1. `NN-<scenario>-<idx>-<client-name>` (e.g. "04-1kb-200p-7-python-async-librdkafka")
      2. anything containing `acksN` (e.g. "rust-native-acks0", "...-acks1-rerun")
      3. anything ending in `<N>min` (e.g. "1b-rust-native-10min")
    Falls back to None (grouped only by parent, as a single default group).
    """
    parts = name.split("-")
    single_digit_positions = [i for i, p in enumerate(parts) if p.isdigit() and len(p) == 1]
    if len(parts) > 3 and single_digit_positions:
        idx = single_digit_positions[-1]
        if 0 < idx < len(parts) - 1:
            tag = "-".join(parts[1:idx])
            if tag:
                return tag

    m = re.search(r"acks\d+", name)
    if m:
        return m.group(0)

    m = re.search(r"(\d+min)$", name)
    if m:
        return m.group(1)

    return None


def plot_single(case_dir: Path, data, out_path: Path) -> bool:
    if not data:
        return False
    fig, axes = plt.subplots(2, 2, figsize=(11, 7), dpi=130)
    fig.suptitle(case_dir.name, fontsize=12, fontweight="bold")
    t = [r["t_min"] for r in data]

    ax = axes[0][0]
    ax.plot(t, [r["p50"] for r in data], label="p50", color="#2f7d6b", linewidth=1.1)
    ax.plot(t, [r["p90"] for r in data], label="p90", color="#5b6fd8", linewidth=1.1)
    ax.plot(t, [r["p99"] for r in data], label="p99", color="#c2477a", linewidth=1.1)
    ax.plot(t, [r["avg"] for r in data], label="avg", color="#8a8f27", linewidth=1.1, linestyle="--")
    ax.set_title("Latency (ms)", fontsize=10)
    ax.legend(fontsize=8)

    ax = axes[0][1]
    ax.plot(t, [r["thpt"] for r in data], color="#3f9bc4", linewidth=1.1)
    ax.set_title("Throughput (msg/s)", fontsize=10)

    ax = axes[1][0]
    ax.plot(t, [r["rss"] for r in data], color="#a35d1f", linewidth=1.1)
    ax.set_title("RSS (MiB)", fontsize=10)

    axes[1][1].axis("off")

    for ax in (axes[0][0], axes[0][1], axes[1][0]):
        ax.set_xlabel("minutes since first traffic", fontsize=8.5)
        ax.grid(True, linewidth=0.4, alpha=0.5)
        ax.tick_params(labelsize=8)

    plt.tight_layout(rect=(0, 0, 1, 0.94))
    plt.savefig(out_path)
    plt.close(fig)
    return True


def plot_comparison(group_dir: Path, group_label: str, cases: dict, out_path: Path) -> bool:
    plottable = {label: data for label, data in cases.items() if data}
    if len(plottable) < 2:
        return False
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), dpi=140)
    fig.suptitle(f"{group_dir.name} / {group_label}" if group_label else group_dir.name,
                 fontsize=12, fontweight="bold")
    colors = {label: CLIENT_COLORS[i % len(CLIENT_COLORS)] for i, label in enumerate(sorted(plottable))}

    for label, data in sorted(plottable.items()):
        t = [r["t_min"] for r in data]
        axes[0].plot(t, [r["p50"] for r in data], label=label, color=colors[label], linewidth=1.1)
        axes[1].plot(t, [r["p99"] for r in data], label=label, color=colors[label], linewidth=1.1)
        axes[2].plot(t, [r["thpt"] for r in data], label=label, color=colors[label], linewidth=1.1)

    axes[0].set_title("p50 latency (ms)", fontsize=10)
    axes[1].set_title("p99 latency (ms)", fontsize=10)
    axes[2].set_title("Throughput (msg/s)", fontsize=10)
    for ax in axes:
        ax.set_xlabel("minutes since first traffic", fontsize=8.5)
        ax.grid(True, linewidth=0.4, alpha=0.5)
        ax.tick_params(labelsize=8)
    axes[2].legend(fontsize=7, loc="best")

    plt.tight_layout(rect=(0, 0, 1, 0.90))
    plt.savefig(out_path)
    plt.close(fig)
    return True


def discover_case_dirs(root: Path):
    for dirpath, dirnames, _filenames in os.walk(root):
        d = Path(dirpath)
        if is_excluded(d):
            dirnames[:] = []
            continue
        dirnames[:] = [dn for dn in dirnames if not is_excluded(d / dn)]
        mf = find_metrics_file(d)
        if mf is not None:
            yield d, mf


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    roots = [Path(p).expanduser().resolve() for p in sys.argv[1:]]
    # (parent_dir, scenario_key) -> {case_label: data}
    groups = defaultdict(dict)
    n_single = 0

    for root in roots:
        if not root.exists():
            print(f"skip (not found): {root}")
            continue
        for case_dir, metrics_file in discover_case_dirs(root):
            bucket_s = pick_bucket_seconds(metrics_file)
            data = load_bucketed(metrics_file, bucket_s)
            out_path = case_dir / "graph.png"
            if plot_single(case_dir, data, out_path):
                n_single += 1
                print(f"  wrote {out_path}")
            key = (case_dir.parent, scenario_group_key(case_dir.name))
            groups[key][case_dir.name] = data

    n_combo = 0
    for (parent, scenario_key), cases in groups.items():
        if len(cases) < 2:
            continue
        suffix = f".{scenario_key}" if scenario_key else ""
        out_path = parent / f"comparison{suffix}.png"
        if plot_comparison(parent, scenario_key or "", cases, out_path):
            n_combo += 1
            print(f"  wrote {out_path}")

    print(f"\nDone: {n_single} per-case graphs, {n_combo} comparison graphs.")


if __name__ == "__main__":
    main()
