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
/// The numeric snapshot a <see cref="Bucket"/> yields at rollover, Python's <c>Bucket.rollover()</c> dict
/// (average, max, total, count) plus the latency percentiles when the bucket tracks a histogram.
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

    internal double Average { get; }

    internal double Max { get; }

    internal double Total { get; }

    internal long Count { get; }

    internal bool HasHistogram { get; }

    internal long P50 { get; }

    internal long P90 { get; }

    internal long P95 { get; }

    internal long P99 { get; }

    internal long P999 { get; }
}

/// <summary>
/// A per-window accumulator (total, count, max), Python's <c>Bucket</c>, or with a histogram its <c>LatencyBucket</c>.
/// Measurements and the sampler's rollover run on different threads, so both take a lock.
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
