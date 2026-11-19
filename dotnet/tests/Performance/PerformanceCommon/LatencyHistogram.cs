// Copyright 2025 Confluent Inc.
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

using System;
using System.Collections.Generic;

namespace Confluent.Kafka.Performance;

/// <summary>
/// The 1 ms-resolution latency histogram and percentile of Python's <c>percentile_from_hist</c>: each measurement is
/// floored and clamped into a <c>0..topBucket</c> bucket. The producer's top bucket is the overflow bucket after
/// <c>MaxLatencyMs</c>; the consumer's is <c>ConsumerMaxLatencyMs</c>, since a backlog's latency can far exceed 10 s.
/// </summary>
internal static class LatencyHistogram
{
    internal const int MaxLatencyMs = 10000;

    internal const int ProducerTopBucket = MaxLatencyMs + 1;

    // Python's CONSUMER_MAX_LATENCY_MS.
    internal const int ConsumerMaxLatencyMs = 600_000;

    internal const int Length = ProducerTopBucket + 1;

    internal static long[] New() => new long[Length];

    internal static long[] New(int topBucket) => new long[topBucket + 1];

    internal static long Bucket(double measurementMs, int topBucket) =>
        Math.Min(Math.Max((long)Math.Floor(measurementMs), 0L), topBucket);

    internal static void Record(long[] hist, double measurementMs) => hist[Bucket(measurementMs, hist.Length - 1)]++;

    internal static long PercentileFromHist(long[] hist, double p)
    {
        long total = 0;
        for (int i = 0; i < hist.Length; i++)
        {
            total += hist[i];
        }

        if (total == 0)
        {
            return 0;
        }

        double target = p * total;
        long cumulative = 0;
        for (int ms = 0; ms < hist.Length; ms++)
        {
            cumulative += hist[ms];
            if (cumulative >= target)
            {
                return ms;
            }
        }

        return hist.Length - 1;
    }

    /// <summary>
    /// The percentile of a histogram held as a dense array of the low buckets plus a sparse map of the buckets above
    /// them, Python's sparse <c>{bucket: count}</c> form.
    /// </summary>
    internal static long PercentileFromHist(long[] dense, IReadOnlyDictionary<long, long>? sparse, double p)
    {
        if (sparse is null || sparse.Count == 0)
        {
            return PercentileFromHist(dense, p);
        }

        long total = 0;
        for (int i = 0; i < dense.Length; i++)
        {
            total += dense[i];
        }

        foreach (long count in sparse.Values)
        {
            total += count;
        }

        double target = p * total;
        long cumulative = 0;
        for (int ms = 0; ms < dense.Length; ms++)
        {
            cumulative += dense[ms];
            if (cumulative >= target)
            {
                return ms;
            }
        }

        var buckets = new List<long>(sparse.Keys);
        buckets.Sort();
        foreach (long bucket in buckets)
        {
            cumulative += sparse[bucket];
            if (cumulative >= target)
            {
                return bucket;
            }
        }

        return buckets[buckets.Count - 1];
    }
}
