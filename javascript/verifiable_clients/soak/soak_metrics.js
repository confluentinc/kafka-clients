// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

// Metric primitives for the producer soak, the JavaScript twin of
// python/soak/soak_metrics.py and of the counters and gauges its SoakMetrics
// adds. The JSONL record schema is the Python soak's (and so the performance
// harness's): the rss / cpu / latency / bytes / messages blocks, the window
// timestamps, then prefix, tags, counters and gauges, every number of a
// bucket written as a string, so the soaks of the bindings are read by the
// same tooling. Keep it identical, or say in the commit why it diverges.

const fs = require('node:fs');

// The latency histogram's range, in 1 ms buckets (python's MAX_LATENCY_MS).
const MAX_LATENCY_MS = 10000;

/**
 * The smallest latency-ms bucket whose cumulative count reaches the p-th
 * percentile (0 < p <= 1); 0 for an empty histogram.
 */
function percentileFromHist(hist, p) {
  let total = 0;
  for (const count of hist) total += count;
  if (total === 0) return 0;
  const target = p * total;
  let cumulative = 0;
  for (let ms = 0; ms < hist.length; ms++) {
    cumulative += hist[ms];
    if (cumulative >= target) return ms;
  }
  return hist.length - 1;
}

/** Python's str() of a float or int, as the JSONL writes bucket numbers. */
function str(value) {
  if (value === -Infinity) return '-inf';
  if (value === Infinity) return 'inf';
  return String(value);
}

class Bucket {
  constructor() {
    this.total = 0;
    this.count = 0;
    this.max = -Infinity;
  }

  addMeasurement(measurement) {
    this.total += measurement;
    this.count += 1;
    if (measurement > this.max) this.max = measurement;
  }

  rollover() {
    const record = {
      average: str(this.count === 0 ? 0 : this.total / this.count),
      max: str(this.max),
      total: str(this.total),
      count: str(this.count),
    };
    this.total = 0;
    this.count = 0;
    this.max = -Infinity;
    return record;
  }
}

/** A Bucket that also reports p50/p90/p99/p999 from a 1 ms histogram. */
class LatencyBucket extends Bucket {
  constructor() {
    super();
    this.hist = new Uint32Array(MAX_LATENCY_MS + 2);
  }

  addMeasurement(measurement) {
    super.addMeasurement(measurement);
    this.hist[Math.min(Math.max(Math.trunc(measurement), 0), MAX_LATENCY_MS + 1)] += 1;
  }

  rollover() {
    const record = super.rollover();
    record.p50 = str(percentileFromHist(this.hist, 0.5));
    record.p90 = str(percentileFromHist(this.hist, 0.9));
    record.p99 = str(percentileFromHist(this.hist, 0.99));
    record.p999 = str(percentileFromHist(this.hist, 0.999));
    this.hist = new Uint32Array(MAX_LATENCY_MS + 2);
    return record;
  }
}

/**
 * The soak's counters and gauges on top of the per-window buckets, appended
 * to a JSONL file, one object per window (`rollover()`): counters as
 * `{total, delta}`, gauges as bucket rollups (latency gauges with
 * percentiles). The file is opened for appending, so a restarted soak adds to
 * the series, and each record is written synchronously, so a `kill -9` loses
 * no window already rolled.
 */
class SoakMetrics {
  /** Gauges recorded through the 1 ms histogram rather than a plain bucket. */
  static LATENCY_GAUGES = new Set(['producer.latency']);

  constructor({ path, baseTags, prefix }) {
    this.prefix = prefix;
    this.baseTags = { ...baseTags };
    this.counters = new Map();
    this.countersAtLastRollover = new Map();
    this.gauges = new Map();
    this.rss = new Bucket();
    this.cpu = new Bucket();
    this.latency = new LatencyBucket();
    this.bytes = new Bucket();
    this.messages = new Bucket();
    this.windowStartMs = Date.now();
    this.lastCpu = process.cpuUsage();
    this.lastCpuAt = performance.now();
    this.fd = path === null ? null : fs.openSync(path, 'a');
  }

  static key(name, tags) {
    if (!tags || Object.keys(tags).length === 0) return name;
    const parts = Object.keys(tags)
      .sort()
      .map((k) => `${k}=${tags[k]}`);
    return `${name}{${parts.join(',')}}`;
  }

  incrCounter(name, incrval, tags = null) {
    const key = SoakMetrics.key(name, tags);
    this.counters.set(key, (this.counters.get(key) || 0) + incrval);
  }

  counter(name, tags = null) {
    return this.counters.get(SoakMetrics.key(name, tags)) || 0;
  }

  setGauge(name, value, tags = null) {
    const key = SoakMetrics.key(name, tags);
    let bucket = this.gauges.get(key);
    if (bucket === undefined) {
      bucket = SoakMetrics.LATENCY_GAUGES.has(name) ? new LatencyBucket() : new Bucket();
      this.gauges.set(key, bucket);
    }
    bucket.addMeasurement(value);
  }

  /** One delivered record of `sizeBytes`, delivered `latencyMs` after its send. */
  observeMessage(sizeBytes, latencyMs) {
    this.messages.addMeasurement(1);
    this.bytes.addMeasurement(sizeBytes);
    if (latencyMs !== null) this.latency.addMeasurement(latencyMs);
  }

  /** Swaps out the window's buckets and returns the window's record. */
  rollover() {
    // The process's RSS and CPU, sampled once per window, as python's
    // MemoryBucket / CPUBucket (psutil's cpu_percent since the last sample).
    this.rss.addMeasurement(process.memoryUsage().rss);
    const now = performance.now();
    const cpu = process.cpuUsage(this.lastCpu);
    const elapsedUs = (now - this.lastCpuAt) * 1000;
    this.cpu.addMeasurement(elapsedUs > 0 ? (100 * (cpu.user + cpu.system)) / elapsedUs : 0);
    this.lastCpu = process.cpuUsage();
    this.lastCpuAt = now;

    const windowStartMs = this.windowStartMs;
    this.windowStartMs = Date.now();
    const latency = this.latency;
    const bytes = this.bytes;
    const messages = this.messages;
    this.latency = new LatencyBucket();
    this.bytes = new Bucket();
    this.messages = new Bucket();
    const counters = {};
    for (const [key, total] of this.counters) {
      counters[key] = { total, delta: total - (this.countersAtLastRollover.get(key) || 0) };
    }
    this.countersAtLastRollover = new Map(this.counters);
    const gauges = {};
    for (const [key, bucket] of this.gauges) gauges[key] = bucket.rollover();
    return {
      rss: this.rss.rollover(),
      cpu: this.cpu.rollover(),
      latency: latency.rollover(),
      bytes: bytes.rollover(),
      messages: messages.rollover(),
      window_start_ms: String(windowStartMs),
      window_end_ms: String(this.windowStartMs),
      measurement_start_ms: '-inf',
      measurement_end_ms: '-inf',
      prefix: this.prefix,
      tags: this.baseTags,
      counters,
      gauges,
    };
  }

  /** Appends one window's record to the file. */
  writeRecord(record) {
    if (this.fd === null) return;
    fs.writeSync(this.fd, `${JSON.stringify(record)}\n`);
  }

  /** Writes the current window and closes the file. Safe to call twice. */
  close() {
    if (this.fd === null) return;
    this.writeRecord(this.rollover());
    fs.closeSync(this.fd);
    this.fd = null;
  }
}

module.exports = { Bucket, LatencyBucket, MAX_LATENCY_MS, SoakMetrics, percentileFromHist };
