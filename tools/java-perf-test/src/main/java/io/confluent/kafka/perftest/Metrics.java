// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package io.confluent.kafka.perftest;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-second metrics aggregator and metrics.jsonl writer.
 *
 * Matches the schema produced by
 * {@code bindings/python/test/performance/performance_common.py} so that the
 * same plotting / analysis tooling
 * ({@code tools/performance_metrics_plot/plot_metrics.py}) works on the Java
 * test's output. Each rolled-over bucket is rendered as four string fields:
 * {@code average}, {@code max}, {@code total}, {@code count}. Empty bucket max
 * is the literal string {@code "-inf"} (matches Python's {@code str(-math.inf)}).
 */
public class Metrics {

    /** Fixed-window bucket. Mirrors {@code performance_common.py:Bucket}. */
    public static class Bucket {
        private double total = 0;
        private long count = 0;
        private double max = Double.NEGATIVE_INFINITY;

        public synchronized void addMeasurement(double measurement) {
            total += measurement;
            count++;
            if (measurement > max) {
                max = measurement;
            }
        }

        /** Snapshot + reset. Returns a string-valued map ready for JSON. */
        public synchronized Map<String, String> rollover() {
            double avg = count == 0 ? 0 : total / count;
            String maxStr = (max == Double.NEGATIVE_INFINITY) ? "-inf" : numericString(max);
            String totalStr = numericString(total);
            Map<String, String> result = new LinkedHashMap<>();
            // Use BigDecimal toPlainString to avoid Java's scientific notation
            // for large doubles (e.g. 1.82E8 instead of 182000000.0). Matches
            // Python's str(float) plain decimal formatting.
            result.put("average", BigDecimal.valueOf(avg).toPlainString());
            result.put("max", maxStr);
            result.put("total", totalStr);
            result.put("count", Long.toString(count));
            // Reset
            total = 0;
            count = 0;
            max = Double.NEGATIVE_INFINITY;
            return result;
        }
    }

    /**
     * Bucket that takes one measurement on every rollover (RSS / CPU sampling).
     * Mirrors {@code performance_common.py:SingleMeasurementBucket}.
     */
    public abstract static class SingleMeasurementBucket extends Bucket {
        protected abstract double sample();

        @Override
        public synchronized Map<String, String> rollover() {
            addMeasurement(sample());
            return super.rollover();
        }
    }

    /** Reads VmRSS from /proc/self/status (Linux) — bytes. */
    public static class MemoryBucket extends SingleMeasurementBucket {
        @Override
        protected double sample() {
            try {
                List<String> lines = Files.readAllLines(Paths.get("/proc/self/status"));
                for (String line : lines) {
                    if (line.startsWith("VmRSS:")) {
                        // "VmRSS:    123456 kB"
                        String[] parts = line.split("\\s+");
                        return Long.parseLong(parts[1]) * 1024L;
                    }
                }
            } catch (IOException e) {
                // Fall through to JVM heap as a portable fallback.
            }
            Runtime r = Runtime.getRuntime();
            return r.totalMemory() - r.freeMemory();
        }
    }

    /**
     * Process CPU percent from {@code /proc/self/stat}, computed as an interval
     * rate over each window — matches the C and Rust perf tests exactly
     * (100% = one core fully used; can exceed 100% across cores). NOT the JVM's
     * {@code getProcessCpuLoad()}, which reports a fraction of total machine
     * capacity (max 100% = all cores) and so is on a different scale.
     */
    public static class CPUBucket extends SingleMeasurementBucket {
        private static final double USER_HZ = 100.0; // Linux clock ticks per second
        private long prevTicks = readBusyTicks();
        private long prevNanos = System.nanoTime();

        @Override
        protected double sample() {
            long ticks = readBusyTicks();
            long now = System.nanoTime();
            double dt = (now - prevNanos) / 1e9;
            long dticks = Math.max(0, ticks - prevTicks);
            prevTicks = ticks;
            prevNanos = now;
            return dt > 0 ? (dticks / USER_HZ) / dt * 100.0 : 0.0;
        }

        /** utime+stime (clock ticks) from /proc/self/stat. */
        private static long readBusyTicks() {
            try {
                String stat = Files.readString(Paths.get("/proc/self/stat"));
                // The comm field (field 2) is parenthesized and may contain
                // spaces, so parse after the final ')'. utime is field 14,
                // stime field 15 — indices 11 and 12 of the split remainder.
                int rparen = stat.lastIndexOf(')');
                if (rparen < 0) return 0;
                String[] f = stat.substring(rparen + 1).trim().split("\\s+");
                return Long.parseLong(f[11]) + Long.parseLong(f[12]);
            } catch (Exception e) {
                return 0;
            }
        }
    }

    /**
     * Latency bucket that also keeps a per-window histogram (ms resolution) so
     * each rollover emits p50/p90/p99/p999 alongside average/max/total/count.
     * The histogram is reset on every rollover. Matches the C and Rust tests.
     */
    public static class LatencyBucket extends Bucket {
        static final int MAX_LATENCY_MS = 10_000;
        private final long[] hist = new long[MAX_LATENCY_MS + 2];

        @Override
        public synchronized void addMeasurement(double measurement) {
            super.addMeasurement(measurement);
            int idx = (int) Math.min(Math.max(measurement, 0), MAX_LATENCY_MS + 1);
            hist[idx]++;
        }

        @Override
        public synchronized Map<String, String> rollover() {
            Map<String, String> r = super.rollover();  // average/max/total/count (+ resets)
            r.put("p50", Long.toString(percentile(hist, 0.50)));
            r.put("p90", Long.toString(percentile(hist, 0.90)));
            r.put("p99", Long.toString(percentile(hist, 0.99)));
            r.put("p999", Long.toString(percentile(hist, 0.999)));
            java.util.Arrays.fill(hist, 0L);  // reset the per-window histogram
            return r;
        }

        private static long percentile(long[] hist, double p) {
            long total = 0;
            for (long c : hist) total += c;
            if (total == 0) return 0;
            long target = (long) Math.ceil(total * p);
            long cum = 0;
            for (int i = 0; i < hist.length; i++) {
                cum += hist[i];
                if (cum >= target) return i;
            }
            return hist.length - 1;
        }
    }

    public final Bucket rss = new MemoryBucket();
    public final Bucket cpu = new CPUBucket();
    public final Bucket latency = new LatencyBucket();
    public final Bucket bytes = new Bucket();
    public final Bucket messages = new Bucket();

    public volatile long measurementStartMs = Long.MIN_VALUE;
    public volatile long measurementEndMs = Long.MIN_VALUE;

    private long windowStartMs = System.currentTimeMillis();
    private final BufferedWriter writer;
    private final ObjectMapper mapper = new ObjectMapper();
    private Thread thread;
    private volatile boolean running = false;

    public int totalExternalMetrics = 0;
    public double totalCpu = 0;
    public double totalRss = 0;
    public Map<String, Object> lastMetrics = null;

    public Metrics() throws IOException {
        // Hard-coded filename to match the Python harness.
        this.writer = Files.newBufferedWriter(Paths.get("metrics.jsonl"));
    }

    public synchronized Map<String, Object> rollover() {
        long previousWindowStart = windowStartMs;
        windowStartMs = System.currentTimeMillis();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("rss", rss.rollover());
        result.put("cpu", cpu.rollover());
        result.put("latency", latency.rollover());
        result.put("bytes", bytes.rollover());
        result.put("messages", messages.rollover());
        result.put("window_start_ms", Long.toString(previousWindowStart));
        result.put("window_end_ms", Long.toString(windowStartMs));
        result.put("measurement_start_ms", measurementStartMs == Long.MIN_VALUE
            ? "-inf" : Long.toString(measurementStartMs));
        result.put("measurement_end_ms", measurementEndMs == Long.MIN_VALUE
            ? "-inf" : Long.toString(measurementEndMs));
        return result;
    }

    public void startCollecting(int intervalSeconds) {
        if (running) return;
        running = true;
        thread = new Thread(() -> {
            try {
                while (running) {
                    Thread.sleep(intervalSeconds * 1000L);
                    Map<String, Object> snapshot = rollover();
                    lastMetrics = snapshot;
                    // The run summary averages CPU/RSS over the measured interval
                    // only (matches the Rust test and plot_metrics.py): measurement
                    // start set and end not yet set. Warmup/cooldown windows are
                    // still written to the JSONL, just excluded from these averages.
                    boolean measured = measurementStartMs != Long.MIN_VALUE
                        && measurementEndMs == Long.MIN_VALUE;
                    if (measured) {
                        @SuppressWarnings("unchecked")
                        Map<String, String> cpuMap = (Map<String, String>) snapshot.get("cpu");
                        @SuppressWarnings("unchecked")
                        Map<String, String> rssMap = (Map<String, String>) snapshot.get("rss");
                        totalCpu += Double.parseDouble(cpuMap.get("average"));
                        totalRss += Double.parseDouble(rssMap.get("average"));
                        totalExternalMetrics++;
                    }
                    String json = mapper.writeValueAsString(snapshot);
                    synchronized (writer) {
                        writer.write(json);
                        writer.newLine();
                        writer.flush();
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (IOException ioe) {
                System.err.println("Metrics writer failed: " + ioe.getMessage());
            }
        }, "metrics-collector");
        thread.setDaemon(true);
        thread.start();
    }

    public void stopCollecting() {
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
        try {
            writer.close();
        } catch (IOException ignored) {
        }
    }

    /**
     * Render a numeric value the way Python's {@code str(x)} would: integers
     * print without a decimal point, floats print with one. Avoids divergence
     * from the Python harness's metrics.jsonl format.
     */
    private static String numericString(double v) {
        if (v == Math.floor(v) && !Double.isInfinite(v) && Math.abs(v) < 1e18) {
            return Long.toString((long) v);
        }
        return Double.toString(v);
    }
}
