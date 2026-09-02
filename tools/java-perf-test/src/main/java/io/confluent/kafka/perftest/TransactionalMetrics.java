// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package io.confluent.kafka.perftest;

import java.io.IOException;
import java.util.Map;

/**
 * {@link Metrics} extended with the transaction-specific per-window buckets
 * written by {@link TransactionalProducerPerformanceTest}: a committed-{@code
 * transactions} throughput bucket and a {@code commit_latency} bucket (with
 * p50/p90/p99/p999 percentiles, same shape as {@code latency}).
 *
 * The extra keys are appended after the standard ones, so {@code metrics.jsonl}
 * stays backward compatible — {@code tools/performance_metrics_plot/
 * plot_metrics.py} ignores unknown fields.
 */
public class TransactionalMetrics extends Metrics {

    /** Committed transactions this window (throughput). */
    public final Bucket transactions = new Bucket();
    /** Per-transaction commit latency (committed only), with percentiles. */
    public final LatencyBucket commitLatency = new LatencyBucket();

    public TransactionalMetrics() throws IOException {
        super();
    }

    @Override
    public synchronized Map<String, Object> rollover() {
        // super.rollover() emits + resets rss/cpu/latency/bytes/messages and the
        // window/measurement markers. Append the transaction buckets after those.
        Map<String, Object> result = super.rollover();
        result.put("transactions", transactions.rollover());
        result.put("commit_latency", commitLatency.rollover());
        return result;
    }
}
