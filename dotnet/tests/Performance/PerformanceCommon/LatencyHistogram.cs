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
/// The 1 ms-resolution latency histogram and percentile of Python's <c>percentile_from_hist</c>: each measurement is
/// clamped into a <c>0..MaxLatencyMs</c> bucket or the one overflow bucket.
/// </summary>
internal static class LatencyHistogram
{
    internal const int MaxLatencyMs = 10000;

    internal const int Length = MaxLatencyMs + 2;

    internal static long[] New() => new long[Length];

    internal static void Record(long[] hist, double measurementMs)
    {
        long idx = Math.Min(Math.Max((long)measurementMs, 0L), MaxLatencyMs + 1L);
        hist[idx]++;
    }

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
