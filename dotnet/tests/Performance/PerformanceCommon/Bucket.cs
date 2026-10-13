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
/// The immutable snapshot a <see cref="Bucket"/> yields at rollover — the numeric form of Python's
/// <c>Bucket.rollover()</c> dict (<c>average</c> / <c>max</c> / <c>total</c> / <c>count</c>), plus the
/// latency percentiles when the bucket tracks a histogram. Kept numeric (not pre-stringified) so
/// <see cref="Metrics"/> can both build the JSON line and accumulate the CPU/RSS averages without a
/// string round-trip (Python re-parses its own string; the result is identical).
/// </summary>
internal readonly struct BucketRollover
{
    internal BucketRollover(double average, double max, double total, long count, bool hasHistogram, long p50, long p90, long p95, long p99, long p999)
    {
        Average = average;
        Max = max;
        Total = total;
        Count = count;
        HasHistogram = hasHistogram;
        P50 = p50;
        P90 = p90;
        P95 = p95;
        P99 = p99;
        P999 = p999;
    }

    /// <summary>Mean of the window's measurements (<c>0</c> when the window had none — Python's int <c>0</c>).</summary>
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

    /// <summary>The window's 95th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P95 { get; }

    /// <summary>The window's 99th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P99 { get; }

    /// <summary>The window's 99.9th-percentile latency (ms); <c>0</c> for a non-histogram bucket.</summary>
    internal long P999 { get; }
}

/// <summary>
/// A per-window accumulator (total / count / max) — the C# analog of Python's <c>Bucket</c>. When
/// constructed with a histogram it additionally tracks the 1 ms latency histogram (the
/// <c>LatencyBucket</c> role) and yields p50/p90/p95/p99/p999 at rollover. Thread-safe: measurements
/// arrive on the producer send / recorder thread while the sampler thread rolls the bucket over, so
/// every mutation and the rollover take a lock (Python leans on the GIL; .NET must lock so a
/// non-atomic <c>double</c> update is never torn).
/// </summary>
internal sealed class Bucket
{
    private readonly object _lock = new object();
    private readonly long[]? _hist;
    private double _total;
    private long _count;
    private double _max = double.NegativeInfinity;

    /// <summary>Creates a bucket, optionally tracking the 1 ms latency histogram (the latency bucket).</summary>
    internal Bucket(bool withHistogram = false)
    {
        _hist = withHistogram ? LatencyHistogram.New() : null;
    }

    /// <summary>
    /// Adds one measurement (Python <c>Bucket.add_measurement</c>): folds it into total / count / max and,
    /// for the latency bucket, into the histogram.
    /// </summary>
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
    /// Snapshots the window and resets to empty (Python <c>Bucket.rollover</c> / <c>LatencyBucket.rollover</c>):
    /// returns average / max / total / count (and, for the latency bucket, the percentiles), then clears
    /// total / count / max (and the histogram) for the next window.
    /// </summary>
    internal BucketRollover Rollover()
    {
        lock (_lock)
        {
            double average = _count == 0 ? 0.0 : _total / _count;
            double max = _max;
            double total = _total;
            long count = _count;

            long p50 = 0, p90 = 0, p95 = 0, p99 = 0, p999 = 0;
            if (_hist is not null)
            {
                p50 = LatencyHistogram.PercentileFromHist(_hist, 0.50);
                p90 = LatencyHistogram.PercentileFromHist(_hist, 0.90);
                p95 = LatencyHistogram.PercentileFromHist(_hist, 0.95);
                p99 = LatencyHistogram.PercentileFromHist(_hist, 0.99);
                p999 = LatencyHistogram.PercentileFromHist(_hist, 0.999);
                Array.Clear(_hist, 0, _hist.Length);
            }

            _total = 0;
            _count = 0;
            _max = double.NegativeInfinity;

            return new BucketRollover(average, max, total, count, _hist is not null, p50, p90, p95, p99, p999);
        }
    }
}
