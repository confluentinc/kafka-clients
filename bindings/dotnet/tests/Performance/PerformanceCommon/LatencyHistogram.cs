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

namespace Confluent.Kafka.Performance;

/// <summary>
/// The shared 1 ms-resolution latency histogram + percentile, matching the C / Rust / Java / Python
/// perf tests exactly (<c>performance_common.py</c> <c>percentile_from_hist</c>). A measurement is
/// clamped into a <c>0..MAX_LATENCY_MS</c> bucket plus one overflow bucket, so the array length is
/// <see cref="Length"/> (<c>MAX_LATENCY_MS + 2</c>).
/// </summary>
internal static class LatencyHistogram
{
    /// <summary>Latency histogram resolution ceiling in ms (1 ms buckets over <c>0..MAX_LATENCY_MS</c>).</summary>
    internal const int MaxLatencyMs = 10000;

    /// <summary>Histogram array length: <c>MAX_LATENCY_MS + 2</c> (the <c>0..MAX</c> buckets plus one overflow).</summary>
    internal const int Length = MaxLatencyMs + 2;

    /// <summary>Allocates a fresh zeroed histogram of length <see cref="Length"/>.</summary>
    internal static long[] New() => new long[Length];

    /// <summary>
    /// Records a latency measurement (ms) into <paramref name="hist"/>, mirroring the Python
    /// <c>idx = min(max(int(measurement), 0), MAX_LATENCY_MS + 1)</c> clamp (truncate toward zero,
    /// floor at 0, cap at the overflow bucket).
    /// </summary>
    internal static void Record(long[] hist, double measurementMs)
    {
        long idx = Math.Min(Math.Max((long)measurementMs, 0L), MaxLatencyMs + 1L);
        hist[idx]++;
    }

    /// <summary>
    /// Returns the smallest latency-ms bucket whose cumulative count reaches the <paramref name="p"/>-th
    /// percentile (<c>0 &lt; p &lt;= 1</c>). Mirrors <c>percentile_from_hist</c>: total <c>0</c> returns
    /// <c>0</c>; otherwise the first bucket index whose running total is <c>&gt;= p * total</c>.
    /// </summary>
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
}
