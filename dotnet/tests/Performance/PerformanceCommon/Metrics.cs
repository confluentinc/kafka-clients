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
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;

namespace Confluent.Kafka.Performance;

/// <summary>The measured interval's CPU and RSS aggregations, Python's <c>external_metrics_aggregations</c>.</summary>
public readonly struct ExternalMetricsAggregations
{
    internal ExternalMetricsAggregations(long totalWindows, double totalCpu, double totalRss)
    {
        TotalExternalMetrics = totalWindows;
        TotalCpu = totalCpu;
        TotalRss = totalRss;
        AverageCpu = totalWindows > 0 ? totalCpu / totalWindows : 0.0;
        AverageRss = totalWindows > 0 ? totalRss / totalWindows : 0.0;
    }

    public long TotalExternalMetrics { get; }

    public double TotalCpu { get; }

    public double TotalRss { get; }

    public double AverageCpu { get; }

    public double AverageRss { get; }
}

/// <summary>
/// The shared metrics harness, Python's <c>Metrics</c>: per-window buckets that a background sampler rolls over each
/// interval into one line of <c>METRICS_FILE</c> (default <c>metrics.jsonl</c>) in Python's schema, and the measured
/// interval's CPU and RSS averages.
/// </summary>
public sealed class Metrics : IDisposable
{
    private const long UnsetMs = long.MinValue;

    private readonly MemorySampler _memorySampler = new MemorySampler();
    private readonly CpuSampler _cpuSampler = new CpuSampler();
    private readonly Bucket _rss = new Bucket();
    private readonly Bucket _cpu = new Bucket();
    private readonly TextWriter _fd;
    private readonly object _writeLock = new object();
    private readonly int _latencyTopBucket;
    private readonly bool _transactional;

    private Bucket _latency;
    private Bucket _bytes = new Bucket();
    private Bucket _messages = new Bucket();
    private Bucket _transactions = new Bucket();
    private Bucket _commitLatency = new Bucket(withHistogram: true);
    private Bucket _abortLatency = new Bucket(withHistogram: true);

    private long _windowStartMs;
    private long _measurementStartMs = UnsetMs;
    private long _measurementEndMs = UnsetMs;

    private long _totalExternalMetrics;
    private double _totalCpu;
    private double _totalRss;
    private double _lastCpuAverage;
    private double _lastRssAverage;
    private bool _hasLastMetrics;

    private Thread? _thread;
    private volatile bool _running;

    public Metrics()
        : this(LatencyHistogram.ProducerTopBucket)
    {
    }

    private Metrics(int latencyTopBucket, bool transactional = false)
        : this(new StreamWriter(PerfEnv.GetString("METRICS_FILE", "metrics.jsonl"), append: false), latencyTopBucket, transactional)
    {
    }

    internal Metrics(TextWriter writer, int latencyTopBucket = LatencyHistogram.ProducerTopBucket, bool transactional = false)
    {
        _fd = writer;
        _latencyTopBucket = latencyTopBucket;
        _transactional = transactional;
        _latency = new Bucket(withHistogram: true, latencyTopBucket);
        _windowStartMs = NowMs();
    }

    /// <summary>
    /// The consumer's metrics, whose per-window latency percentiles clamp at 600,000 ms, as Python's
    /// <c>Metrics(latency_top_bucket=CONSUMER_MAX_LATENCY_MS)</c>.
    /// </summary>
    public static Metrics ForConsumer() => new Metrics(LatencyHistogram.ConsumerMaxLatencyMs);

    /// <summary>
    /// The transactional producer's metrics: each line also carries the window's transactions and its commit and
    /// abort latencies, as Python's transactional <c>Metrics</c>.
    /// </summary>
    public static Metrics ForTransactional() => new Metrics(LatencyHistogram.ProducerTopBucket, transactional: true);

    public static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public void AddLatency(double latencyMs) => Volatile.Read(ref _latency).AddMeasurement(latencyMs);

    public void AddBytes(double count) => Volatile.Read(ref _bytes).AddMeasurement(count);

    public void AddMessages(double count) => Volatile.Read(ref _messages).AddMeasurement(count);

    public void AddTransaction() => Volatile.Read(ref _transactions).AddMeasurement(1);

    public void AddCommitLatency(double latencyMs) => Volatile.Read(ref _commitLatency).AddMeasurement(latencyMs);

    public void AddAbortLatency(double latencyMs) => Volatile.Read(ref _abortLatency).AddMeasurement(latencyMs);

    public void ResetCpuBaseline() => _cpuSampler.ResetBaseline();

    public void SetMeasurementStart(long epochMs) => Volatile.Write(ref _measurementStartMs, epochMs);

    public void SetMeasurementEnd(long epochMs) => Volatile.Write(ref _measurementEndMs, epochMs);

    public void StartCollecting(int intervalSeconds = 1)
    {
        if (_running)
        {
            return;
        }

        _running = true;
        _thread = new Thread(() => CollectorLoop(intervalSeconds))
        {
            IsBackground = true,
            Name = "perf-metrics-sampler",
        };
        _thread.Start();
    }

    public void StopCollecting()
    {
        _running = false;
        Thread? thread = _thread;
        if (thread is not null)
        {
            thread.Join();
            _thread = null;
        }
    }

    public (double LastCpu, double LastRss) ExternalMetricsLastValues()
    {
        lock (_writeLock)
        {
            return _hasLastMetrics ? (_lastCpuAverage, _lastRssAverage) : (0.0, 0.0);
        }
    }

    public ExternalMetricsAggregations ExternalMetricsAggregations()
    {
        lock (_writeLock)
        {
            return new ExternalMetricsAggregations(_totalExternalMetrics, _totalCpu, _totalRss);
        }
    }

    public void Dispose()
    {
        StopCollecting();
        _fd.Flush();
        _fd.Dispose();
    }

    private void CollectorLoop(int intervalSeconds)
    {
        int intervalMs = intervalSeconds * 1000;
        while (_running)
        {
            Thread.Sleep(intervalMs);
            string line = Rollover(out double cpuAverage, out double rssAverage);

            long start = Volatile.Read(ref _measurementStartMs);
            long end = Volatile.Read(ref _measurementEndMs);
            bool inMeasured = start != UnsetMs && end == UnsetMs;

            lock (_writeLock)
            {
                _lastCpuAverage = cpuAverage;
                _lastRssAverage = rssAverage;
                _hasLastMetrics = true;
                if (inMeasured)
                {
                    _totalExternalMetrics++;
                    _totalCpu += cpuAverage;
                    _totalRss += rssAverage;
                }

                _fd.WriteLine(line);
                _fd.Flush();
            }
        }
    }

    private string Rollover(out double cpuAverage, out double rssAverage)
    {
        long windowStart = _windowStartMs;
        _windowStartMs = NowMs();

        // Swapped, as in Python, so new measurements go to a fresh bucket; RSS and CPU are touched
        // only on this thread.
        Bucket latency = Interlocked.Exchange(ref _latency, new Bucket(withHistogram: true, _latencyTopBucket));
        Bucket bytes = Interlocked.Exchange(ref _bytes, new Bucket());
        Bucket messages = Interlocked.Exchange(ref _messages, new Bucket());
        Bucket transactions = Interlocked.Exchange(ref _transactions, new Bucket());
        Bucket commitLatency = Interlocked.Exchange(ref _commitLatency, new Bucket(withHistogram: true));
        Bucket abortLatency = Interlocked.Exchange(ref _abortLatency, new Bucket(withHistogram: true));

        _rss.AddMeasurement(_memorySampler.Sample());
        _cpu.AddMeasurement(_cpuSampler.Sample());

        BucketRollover rssRoll = _rss.Rollover();
        BucketRollover cpuRoll = _cpu.Rollover();
        BucketRollover latencyRoll = latency.Rollover();
        BucketRollover bytesRoll = bytes.Rollover();
        BucketRollover messagesRoll = messages.Rollover();
        BucketRollover transactionsRoll = transactions.Rollover();
        BucketRollover commitLatencyRoll = commitLatency.Rollover();
        BucketRollover abortLatencyRoll = abortLatency.Rollover();

        cpuAverage = cpuRoll.Average;
        rssAverage = rssRoll.Average;

        return BuildLine(
            rssRoll,
            cpuRoll,
            latencyRoll,
            bytesRoll,
            messagesRoll,
            transactionsRoll,
            commitLatencyRoll,
            abortLatencyRoll,
            windowStart,
            _windowStartMs);
    }

    private string BuildLine(
        BucketRollover rss,
        BucketRollover cpu,
        BucketRollover latency,
        BucketRollover bytes,
        BucketRollover messages,
        BucketRollover transactions,
        BucketRollover commitLatency,
        BucketRollover abortLatency,
        long windowStartMs,
        long windowEndMs)
    {
        var sb = new StringBuilder(512);
        sb.Append('{');
        AppendBucket(sb, "rss", rss, first: true);
        AppendBucket(sb, "cpu", cpu, first: false);
        AppendLatencyBucket(sb, "latency", latency);
        AppendBucket(sb, "bytes", bytes, first: false);
        AppendBucket(sb, "messages", messages, first: false);
        AppendStringField(sb, "window_start_ms", PerfFormat.Num(windowStartMs));
        AppendStringField(sb, "window_end_ms", PerfFormat.Num(windowEndMs));
        AppendStringField(sb, "measurement_start_ms", FormatMeasurementBound(Volatile.Read(ref _measurementStartMs)));
        AppendStringField(sb, "measurement_end_ms", FormatMeasurementBound(Volatile.Read(ref _measurementEndMs)));
        if (_transactional)
        {
            // After the base fields, as Python's transactional rollover appends them to the base line.
            AppendBucket(sb, "transactions", transactions, first: false);
            AppendLatencyBucket(sb, "commit_latency", commitLatency);
            AppendLatencyBucket(sb, "abort_latency", abortLatency);
        }

        sb.Append('}');
        return sb.ToString();
    }

    private static string FormatMeasurementBound(long value) =>
        value == UnsetMs ? PerfFormat.NegInf : PerfFormat.Num(value);

    private static void AppendBucket(StringBuilder sb, string key, BucketRollover b, bool first)
    {
        if (!first)
        {
            sb.Append(", ");
        }

        sb.Append('"').Append(key).Append("\": {");
        AppendCoreBucketFields(sb, b);
        sb.Append('}');
    }

    private static void AppendLatencyBucket(StringBuilder sb, string key, BucketRollover b)
    {
        sb.Append(", \"").Append(key).Append("\": {");
        AppendCoreBucketFields(sb, b);
        // No p95, as Python's LatencyBucket.
        sb.Append(", \"p50\": \"").Append(PerfFormat.Num(b.P50)).Append('"');
        sb.Append(", \"p90\": \"").Append(PerfFormat.Num(b.P90)).Append('"');
        sb.Append(", \"p99\": \"").Append(PerfFormat.Num(b.P99)).Append('"');
        sb.Append(", \"p999\": \"").Append(PerfFormat.Num(b.P999)).Append('"');
        sb.Append('}');
    }

    private static void AppendCoreBucketFields(StringBuilder sb, BucketRollover b)
    {
        sb.Append("\"average\": \"").Append(PerfFormat.Num(b.Average)).Append('"');
        sb.Append(", \"max\": \"").Append(PerfFormat.Num(b.Max)).Append('"');
        sb.Append(", \"total\": \"").Append(PerfFormat.Num(b.Total)).Append('"');
        sb.Append(", \"count\": \"").Append(b.Count.ToString(CultureInfo.InvariantCulture)).Append('"');
    }

    private static void AppendStringField(StringBuilder sb, string key, string value)
    {
        sb.Append(", \"").Append(key).Append("\": \"").Append(value).Append('"');
    }
}
