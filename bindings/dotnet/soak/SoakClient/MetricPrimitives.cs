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
// PROVENANCE: forked from bindings/dotnet/tests/Performance/PerformanceCommon/
// (Bucket.cs, LatencyHistogram.cs, PerfFormat.cs) and DELIBERATELY DUPLICATED rather
// than referenced. Two independent reasons, either sufficient:
//
//  1. Mechanical. Five of the seven primitives the compose plan named are `internal`
//     to PerformanceCommon (Bucket, LatencyHistogram, PerfFormat, MemorySampler,
//     CpuSampler), and its only public Metrics constructor is `Metrics()` — no path,
//     no append mode, which are precisely the two things the soak needs. Reaching
//     them would mean widening a TEST-harness assembly's surface to serve an
//     operational tool.
//  2. It is what the Python sibling deliberately did, for a stated reason
//     (bindings/python/soak/soak_metrics.py's PROVENANCE header): a two-week run must
//     not break because a performance-test refactor changed a shared helper, and the
//     soak's own needs (append mode, a promptly-stoppable collector) must not distort
//     code the perf tests depend on.
//
// ⚠ THE JSONL RECORD SCHEMA MUST STAY IDENTICAL TO THE PERF HARNESS'S. That schema is
// what makes soak and perf numbers directly comparable across clients and languages
// (tools/performance_metrics_plot parses every language's output identically). If you
// change the shape of a record — the rss/cpu/latency/bytes/messages blocks, the window
// timestamps, or the bucket fields — change PerformanceCommon/Metrics.cs the same way,
// or say explicitly in the commit why the two are diverging.

using System;
using System.Globalization;

namespace Confluent.Kafka.Soak;

/// <summary>
/// The shared 1 ms-resolution latency histogram + percentile, matching the C / Rust /
/// Java / Python perf tests exactly (<c>performance_common.py</c>
/// <c>percentile_from_hist</c>, and <c>soak_metrics.py</c>'s copy of it).
/// </summary>
internal static class LatencyHistogram
{
    /// <summary>Latency histogram resolution ceiling in ms (1 ms buckets over <c>0..MaxLatencyMs</c>).</summary>
    internal const int MaxLatencyMs = 10000;

    /// <summary>Histogram array length: <c>MaxLatencyMs + 2</c> (the <c>0..MAX</c> buckets plus one overflow).</summary>
    internal const int Length = MaxLatencyMs + 2;

    /// <summary>Allocates a fresh zeroed histogram of length <see cref="Length"/>.</summary>
    internal static long[] New() => new long[Length];

    /// <summary>
    /// Records a latency measurement (ms), mirroring Python's
    /// <c>idx = min(max(int(measurement), 0), MAX_LATENCY_MS + 1)</c> clamp.
    /// </summary>
    internal static void Record(long[] hist, double measurementMs)
    {
        long idx = Math.Min(Math.Max((long)measurementMs, 0L), MaxLatencyMs + 1L);
        hist[idx]++;
    }

    /// <summary>
    /// Returns the smallest latency-ms bucket whose cumulative count reaches the
    /// <paramref name="p"/>-th percentile (<c>0 &lt; p &lt;= 1</c>).
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

/// <summary>
/// Numeric formatting for the shared <c>metrics.jsonl</c> schema. The plot tool floats
/// every numeric field and only special-cases the string <c>"-inf"</c>, so the hard
/// contract is: emit <c>"-inf"</c> for an unset maximum / measurement bound, and
/// invariant-culture numeric text everywhere else — never a locale-specific separator.
/// </summary>
internal static class SoakFormat
{
    /// <summary>The <c>-inf</c> sentinel emitted for an unset max / measurement bound.</summary>
    internal const string NegInf = "-inf";

    /// <summary>Formats a metric value as its <c>metrics.jsonl</c> string.</summary>
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

    /// <summary>Formats a long as invariant-culture decimal text.</summary>
    internal static string Num(long value) => value.ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// The immutable snapshot a <see cref="Bucket"/> yields at rollover — the numeric form
/// of Python's <c>Bucket.rollover()</c> dict (average / max / total / count), plus the
/// latency percentiles when the bucket tracks a histogram.
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

    /// <summary>Mean of the window's measurements (<c>0</c> when the window had none).</summary>
    internal double Average { get; }

    /// <summary>Maximum measurement (<see cref="double.NegativeInfinity"/> — the <c>"-inf"</c> sentinel — when the window had none).</summary>
    internal double Max { get; }

    /// <summary>Sum of the window's measurements.</summary>
    internal double Total { get; }

    /// <summary>Number of measurements in the window.</summary>
    internal long Count { get; }

    /// <summary>Whether this bucket tracks a latency histogram (so the percentiles below are meaningful).</summary>
    internal bool HasHistogram { get; }

    /// <summary>The window's 50th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P50 { get; }

    /// <summary>The window's 90th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P90 { get; }

    /// <summary>The window's 99th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P99 { get; }

    /// <summary>The window's 99.9th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P999 { get; }
}

/// <summary>
/// A per-window accumulator (total / count / max) — the C# analog of Python's
/// <c>Bucket</c>. Constructed with a histogram it additionally tracks the 1 ms latency
/// histogram (the <c>LatencyBucket</c> role) and yields p50/p90/p99/p999 at rollover.
/// Thread-safe: measurements arrive on the producer / consumer / delivery-continuation
/// threads while the sampler thread rolls the bucket over (Python leans on the GIL;
/// .NET must lock so a non-atomic <c>double</c> update is never torn).
/// </summary>
internal sealed class Bucket
{
    private readonly object _lock = new object();
    private readonly long[]? _hist;
    private double _total;
    private long _count;
    private double _max = double.NegativeInfinity;

    /// <summary>Creates a bucket, optionally tracking the 1 ms latency histogram.</summary>
    internal Bucket(bool withHistogram = false)
    {
        _hist = withHistogram ? LatencyHistogram.New() : null;
    }

    /// <summary>Adds one measurement (Python <c>Bucket.add_measurement</c>).</summary>
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

    /// <summary>
    /// Snapshots the window and resets to empty (Python <c>Bucket.rollover</c> /
    /// <c>LatencyBucket.rollover</c>).
    /// </summary>
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
