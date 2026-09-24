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

/// <summary>
/// Aggregations over the measured interval — the C# analog of Python's
/// <c>external_metrics_aggregations()</c>.
/// </summary>
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

    /// <summary>Number of measured-interval windows that contributed to the averages.</summary>
    public long TotalExternalMetrics { get; }

    /// <summary>Sum of the per-window average CPU (%).</summary>
    public double TotalCpu { get; }

    /// <summary>Sum of the per-window average RSS (bytes).</summary>
    public double TotalRss { get; }

    /// <summary>Mean CPU (%) over the measured interval, or <c>0</c> if no window contributed.</summary>
    public double AverageCpu { get; }

    /// <summary>Mean RSS (bytes) over the measured interval, or <c>0</c> if no window contributed.</summary>
    public double AverageRss { get; }
}

/// <summary>
/// The shared metrics harness — the C# analog of <c>performance_common.py</c>'s <see cref="Metrics"/>.
/// Owns the per-window accumulators (RSS / CPU / latency / bytes / messages), a background sampler
/// thread that rolls them over once per interval and appends one JSON object per line to
/// <c>metrics.jsonl</c>, and the measured-interval bounds that gate which windows feed the CPU/RSS
/// averages. The <c>metrics.jsonl</c> schema is kept byte-compatible with the Python / Rust siblings
/// (see <see cref="PerfFormat"/>) so <c>tools/performance_metrics_plot</c> parses every language's
/// output identically.
/// </summary>
public sealed class Metrics : IDisposable
{
    /// <summary>Sentinel for an unset measurement bound — serialized as the <c>"-inf"</c> string.</summary>
    private const long UnsetMs = long.MinValue;

    private readonly MemorySampler _memorySampler = new MemorySampler();
    private readonly CpuSampler _cpuSampler = new CpuSampler();
    private readonly Bucket _rss = new Bucket();
    private readonly Bucket _cpu = new Bucket();
    private readonly TextWriter _fd;
    private readonly object _writeLock = new object();

    private Bucket _latency = new Bucket(withHistogram: true);
    private Bucket _bytes = new Bucket();
    private Bucket _messages = new Bucket();

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

    /// <summary>Creates the harness and truncates / opens <c>metrics.jsonl</c> in the current directory.</summary>
    public Metrics()
        : this(new StreamWriter("metrics.jsonl", append: false))
    {
    }

    /// <summary>Creates the harness writing JSONL lines to <paramref name="writer"/> (used by tests).</summary>
    internal Metrics(TextWriter writer)
    {
        _fd = writer;
        _windowStartMs = NowMs();
    }

    /// <summary>Wall-clock epoch milliseconds (Python's <c>int(time.time() * 1000)</c>).</summary>
    public static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>Records a per-record latency (ms) into the current window's latency bucket.</summary>
    public void AddLatency(double latencyMs) => Volatile.Read(ref _latency).AddMeasurement(latencyMs);

    /// <summary>Records <paramref name="count"/> bytes into the current window's bytes bucket.</summary>
    public void AddBytes(double count) => Volatile.Read(ref _bytes).AddMeasurement(count);

    /// <summary>Records <paramref name="count"/> messages into the current window's messages bucket.</summary>
    public void AddMessages(double count) => Volatile.Read(ref _messages).AddMeasurement(count);

    /// <summary>Marks the start of the measured interval (epoch ms); windows from here feed the averages.</summary>
    public void SetMeasurementStart(long epochMs) => Volatile.Write(ref _measurementStartMs, epochMs);

    /// <summary>Marks the end of the measured interval (epoch ms); later windows are cooldown and excluded.</summary>
    public void SetMeasurementEnd(long epochMs) => Volatile.Write(ref _measurementEndMs, epochMs);

    /// <summary>
    /// Starts the background sampler thread (Python <c>start_collecting</c>). Idempotent. The thread is a
    /// background thread so a crash before <see cref="StopCollecting"/> still lets the process exit
    /// (mirroring Python's <c>daemon=True</c>); a normal shutdown joins it via <see cref="StopCollecting"/>.
    /// </summary>
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

    /// <summary>Stops the sampler thread and joins it (Python <c>stop_collecting</c>).</summary>
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

    /// <summary>Returns the last window's CPU / RSS averages (Python <c>external_metrics_last_values</c>).</summary>
    public (double LastCpu, double LastRss) ExternalMetricsLastValues()
    {
        lock (_writeLock)
        {
            return _hasLastMetrics ? (_lastCpuAverage, _lastRssAverage) : (0.0, 0.0);
        }
    }

    /// <summary>Returns the measured-interval CPU / RSS aggregations (Python <c>external_metrics_aggregations</c>).</summary>
    public ExternalMetricsAggregations ExternalMetricsAggregations()
    {
        lock (_writeLock)
        {
            return new ExternalMetricsAggregations(_totalExternalMetrics, _totalCpu, _totalRss);
        }
    }

    /// <inheritdoc/>
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

            // Average CPU/RSS only over the measured interval — exclude warmup (measurement not
            // started) and post-test cooldown (measurement ended), matching the C/Rust/Java/Python
            // perf tests.
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

        // Swap latency/bytes/messages so the sampler rolls a detached bucket (Python's swap-then-rollover);
        // RSS/CPU are only touched here on the sampler thread, so they need no swap.
        Bucket latency = Interlocked.Exchange(ref _latency, new Bucket(withHistogram: true));
        Bucket bytes = Interlocked.Exchange(ref _bytes, new Bucket());
        Bucket messages = Interlocked.Exchange(ref _messages, new Bucket());

        _rss.AddMeasurement(_memorySampler.Sample());
        _cpu.AddMeasurement(_cpuSampler.Sample());

        BucketRollover rssRoll = _rss.Rollover();
        BucketRollover cpuRoll = _cpu.Rollover();
        BucketRollover latencyRoll = latency.Rollover();
        BucketRollover bytesRoll = bytes.Rollover();
        BucketRollover messagesRoll = messages.Rollover();

        cpuAverage = cpuRoll.Average;
        rssAverage = rssRoll.Average;

        return BuildLine(rssRoll, cpuRoll, latencyRoll, bytesRoll, messagesRoll, windowStart, _windowStartMs);
    }

    private string BuildLine(
        BucketRollover rss,
        BucketRollover cpu,
        BucketRollover latency,
        BucketRollover bytes,
        BucketRollover messages,
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
        // The latency object additionally carries p50/p90/p99/p999 (NOT p95 — the consumer summary
        // computes p95 separately; the JSONL schema omits it, §3.2).
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
