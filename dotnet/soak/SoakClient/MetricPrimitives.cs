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
//
// Forked from PerformanceCommon, not referenced, as Python's soak forks its perf helpers; keep the
// metrics.jsonl record schema identical to PerformanceCommon/Metrics.cs (one plot tool reads both).

using System;
using System.Globalization;

namespace Confluent.Kafka.Soak;

/// <summary>
/// The 1 ms-resolution latency histogram and percentile shared by the C, Rust, Java and Python
/// perf tests (Python's <c>percentile_from_hist</c>).
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

/// <summary>
/// Numeric text for <c>metrics.jsonl</c>: <c>"-inf"</c> for an unset maximum (the only string the
/// plot tool special-cases), invariant-culture numbers everywhere else.
/// </summary>
internal static class SoakFormat
{
    internal const string NegInf = "-inf";

    internal static string Num(double value)
    {
        if (double.IsNegativeInfinity(value))
        {
            return NegInf;
        }

        if (double.IsPositiveInfinity(value))
        {
            return "inf";
        }

        if (double.IsNaN(value))
        {
            return "nan";
        }

        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    internal static string Num(long value) => value.ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// What a <see cref="Bucket"/> yields at rollover, as Python's <c>Bucket.rollover()</c>: average,
/// max, total, count, and the latency percentiles when the bucket tracks a histogram.
/// </summary>
internal readonly struct BucketRollover
{
    internal BucketRollover(double average, double max, double total, long count, bool hasHistogram, long p50, long p90, long p99, long p999)
    {
        Average = average;
        Max = max;
        Total = total;
        Count = count;
        HasHistogram = hasHistogram;
        P50 = p50;
        P90 = p90;
        P99 = p99;
        P999 = p999;
    }

    internal double Average { get; }

    internal double Max { get; }

    internal double Total { get; }

    internal long Count { get; }

    internal bool HasHistogram { get; }

    internal long P50 { get; }

    internal long P90 { get; }

    internal long P99 { get; }

    internal long P999 { get; }
}

/// <summary>
/// A per-window accumulator (total, count, max), as Python's <c>Bucket</c>; with a histogram it
/// also yields the latency percentiles, as <c>LatencyBucket</c>. Locked: measurements and the
/// sampler's rollover run on different threads, where Python relies on the GIL.
/// </summary>
internal sealed class Bucket
{
    private readonly object _lock = new object();
    private readonly long[]? _hist;
    private double _total;
    private long _count;
    private double _max = double.NegativeInfinity;

    internal Bucket(bool withHistogram = false)
    {
        _hist = withHistogram ? LatencyHistogram.New() : null;
    }

    internal void AddMeasurement(double measurement)
    {
        lock (_lock)
        {
            _total += measurement;
            _count++;
            if (measurement > _max)
            {
                _max = measurement;
            }

            if (_hist is not null)
            {
                LatencyHistogram.Record(_hist, measurement);
            }
        }
    }

    internal BucketRollover Rollover()
    {
        lock (_lock)
        {
            double average = _count == 0 ? 0.0 : _total / _count;
            double max = _max;
            double total = _total;
            long count = _count;

            long p50 = 0, p90 = 0, p99 = 0, p999 = 0;
            if (_hist is not null)
            {
                p50 = LatencyHistogram.PercentileFromHist(_hist, 0.50);
                p90 = LatencyHistogram.PercentileFromHist(_hist, 0.90);
                p99 = LatencyHistogram.PercentileFromHist(_hist, 0.99);
                p999 = LatencyHistogram.PercentileFromHist(_hist, 0.999);
                Array.Clear(_hist, 0, _hist.Length);
            }

            _total = 0;
            _count = 0;
            _max = double.NegativeInfinity;

            return new BucketRollover(average, max, total, count, _hist is not null, p50, p90, p99, p999);
        }
    }
}
