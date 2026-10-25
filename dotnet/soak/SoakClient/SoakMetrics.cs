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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Confluent.Kafka.Soak;

/// <summary>
/// The telemetry pipeline <see cref="SoakMetrics"/> drives besides the JSONL file; injectable, so
/// the export-path contracts can be asserted without a collector.
/// </summary>
internal interface ISoakTelemetrySink : IDisposable
{
    void IncrCounter(string fullName, long increment, IReadOnlyDictionary<string, string> tags);

    void SetGauge(string fullName, double value, IReadOnlyDictionary<string, string> tags);

    void Shutdown();
}

/// <summary>
/// The soak's counters and gauges over <see cref="Bucket"/> and <see cref="LatencyHistogram"/>: a
/// rollover thread appends one JSON line per window to the metrics file (always written, appended
/// across <c>run.sh</c> restarts, flushed per write), plus the OTLP pipeline when one is built.
/// </summary>
internal sealed class SoakMetrics : IDisposable
{
    private const long UnsetMs = long.MinValue;

    internal static readonly IReadOnlyCollection<string> LatencyGauges = new HashSet<string>(StringComparer.Ordinal)
    {
        "producer.latency",
        "consumer.e2e_latency",
        "consumer.recovery_ms",
    };

    // Recorded in ms, exported in seconds as Python's soak; converting on record would put every
    // sample in the 0 ms bucket. consumer.recovery_ms stays in ms.
    internal static readonly IReadOnlyCollection<string> SecondsOnExport = new HashSet<string>(StringComparer.Ordinal)
    {
        "producer.latency",
        "consumer.e2e_latency",
    };

    private static readonly IReadOnlyDictionary<string, string> s_emptyTags =
        new Dictionary<string, string>(StringComparer.Ordinal);

    private readonly object _lock = new object();
    private readonly MemorySampler _memorySampler = new MemorySampler();
    private readonly CpuSampler _cpuSampler = new CpuSampler();
    private readonly Bucket _rss = new Bucket();
    private readonly Bucket _cpu = new Bucket();
    private readonly Dictionary<string, long> _counters = new Dictionary<string, long>(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _countersAtLastRollover = new Dictionary<string, long>(StringComparer.Ordinal);
    private readonly Dictionary<string, Bucket> _gauges = new Dictionary<string, Bucket>(StringComparer.Ordinal);
    private readonly IReadOnlyDictionary<string, string> _baseTags;
    private readonly string _prefix;
    private readonly TextWriter _writer;

    // Waited on instead of sleeping, so Stop returns without waiting out the window.
    private readonly ManualResetEventSlim _stop = new ManualResetEventSlim(false);

    private Bucket _latency = new Bucket(withHistogram: true);
    private Bucket _bytes = new Bucket();
    private Bucket _messages = new Bucket();

    private readonly SoakLogger? _logger;

    private ISoakTelemetrySink? _otel;
    private long _windowStartMs;
    private long _measurementStartMs = UnsetMs;
    private long _measurementEndMs = UnsetMs;
    private Thread? _thread;
    private bool _closed;

    internal SoakMetrics(
        string path,
        IReadOnlyDictionary<string, string> baseTags,
        SoakLogger logger,
        string prefix,
        ISoakTelemetrySink? sink)
        : this(new StreamWriter(path, append: true), baseTags, prefix, sink, logger)
    {
    }

    internal SoakMetrics(
        TextWriter writer,
        IReadOnlyDictionary<string, string> baseTags,
        string prefix,
        ISoakTelemetrySink? sink,
        SoakLogger? logger = null)
    {
        _writer = writer;
        _baseTags = baseTags;
        _prefix = prefix;
        _otel = sink;
        _logger = logger;
        _windowStartMs = NowMs();
    }

    internal bool OtelEnabled => _otel is not null;

    internal string Prefix => _prefix;

    internal static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    internal void SetMeasurementStart(long epochMs) => Volatile.Write(ref _measurementStartMs, epochMs);

    internal void SetMeasurementEnd(long epochMs) => Volatile.Write(ref _measurementEndMs, epochMs);

    internal void IncrCounter(string metricName, long increment, IReadOnlyDictionary<string, string>? tags = null)
    {
        string key = MetricKey(metricName, tags);
        lock (_lock)
        {
            _counters.TryGetValue(key, out long current);
            _counters[key] = current + increment;
        }

        ISoakTelemetrySink? sink = _otel;
        sink?.IncrCounter(_prefix + metricName, increment, tags ?? s_emptyTags);
    }

    internal void SetGauge(string metricName, double value, IReadOnlyDictionary<string, string>? tags = null)
    {
        string key = MetricKey(metricName, tags);
        lock (_lock)
        {
            if (!_gauges.TryGetValue(key, out Bucket? bucket))
            {
                bucket = new Bucket(withHistogram: LatencyGauges.Contains(metricName));
                _gauges[key] = bucket;
            }

            bucket.AddMeasurement(value);
        }

        ISoakTelemetrySink? sink = _otel;
        if (sink is not null)
        {
            double exported = SecondsOnExport.Contains(metricName) ? value / 1000.0 : value;
            sink.SetGauge(_prefix + metricName, exported, tags ?? s_emptyTags);
        }
    }

    internal void ObserveMessage(long sizeBytes, double? latencyMs)
    {
        lock (_lock)
        {
            _messages.AddMeasurement(1);
            _bytes.AddMeasurement(sizeBytes);
            if (latencyMs.HasValue)
            {
                _latency.AddMeasurement(latencyMs.Value);
            }
        }
    }

    // A background thread, as Python's daemon=True; a normal shutdown joins it.
    internal void StartCollecting(double intervalSeconds)
    {
        if (_thread is not null)
        {
            return;
        }

        int intervalMs = Math.Max(1, (int)(intervalSeconds * 1000.0));
        _thread = new Thread(() =>
        {
            while (!_stop.Wait(intervalMs))
            {
                // Total: an unhandled exception here would end the process, so a full disk would
                // kill the soak through its own telemetry; log and lose one window instead.
                try
                {
                    WriteRecord(Rollover());
                }
                catch (Exception ex)
                {
                    _logger?.Error("metrics: window rollover failed, continuing: " + ex.Message);
                }
            }
        })
        {
            IsBackground = true,
            Name = "metrics",
        };
        _thread.Start();
    }

    internal void StopCollecting()
    {
        _stop.Set();
        Thread? thread = _thread;
        if (thread is not null)
        {
            thread.Join();
            _thread = null;
        }
    }

    internal void WriteFinal() => WriteRecord(Rollover());

    internal void Close()
    {
        ISoakTelemetrySink? sink = Interlocked.Exchange(ref _otel, null);
        if (sink is not null)
        {
            sink.Shutdown();
            sink.Dispose();
        }

        lock (_lock)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            _writer.Flush();
            _writer.Dispose();
        }
    }

    public void Dispose()
    {
        StopCollecting();
        _stop.Dispose();
        Close();
    }

    internal string Rollover()
    {
        lock (_lock)
        {
            long windowStartMs = _windowStartMs;
            _windowStartMs = NowMs();

            Bucket latency = _latency;
            Bucket bytes = _bytes;
            Bucket messages = _messages;
            _latency = new Bucket(withHistogram: true);
            _bytes = new Bucket();
            _messages = new Bucket();

            _rss.AddMeasurement(_memorySampler.Sample());
            _cpu.AddMeasurement(_cpuSampler.Sample());

            var counters = new Dictionary<string, (long Total, long Delta)>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, long> entry in _counters)
            {
                _countersAtLastRollover.TryGetValue(entry.Key, out long previous);
                counters[entry.Key] = (entry.Value, entry.Value - previous);
            }

            foreach (KeyValuePair<string, long> entry in _counters)
            {
                _countersAtLastRollover[entry.Key] = entry.Value;
            }

            var gauges = new Dictionary<string, BucketRollover>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, Bucket> entry in _gauges)
            {
                gauges[entry.Key] = entry.Value.Rollover();
            }

            return BuildLine(
                _rss.Rollover(),
                _cpu.Rollover(),
                latency.Rollover(),
                bytes.Rollover(),
                messages.Rollover(),
                windowStartMs,
                _windowStartMs,
                counters,
                gauges);
        }
    }

    internal static string MetricKey(string name, IReadOnlyDictionary<string, string>? tags)
    {
        if (tags is null || tags.Count == 0)
        {
            return name;
        }

        string rendered = string.Join(
            ",",
            tags.OrderBy(tag => tag.Key, StringComparer.Ordinal)
                .Select(tag => tag.Key + "=" + tag.Value));
        return name + "{" + rendered + "}";
    }

    private void WriteRecord(string line)
    {
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }

            _writer.WriteLine(line);
            _writer.Flush();
        }
    }

    private string BuildLine(
        BucketRollover rss,
        BucketRollover cpu,
        BucketRollover latency,
        BucketRollover bytes,
        BucketRollover messages,
        long windowStartMs,
        long windowEndMs,
        IReadOnlyDictionary<string, (long Total, long Delta)> counters,
        IReadOnlyDictionary<string, BucketRollover> gauges)
    {
        var sb = new StringBuilder(1024);
        sb.Append('{');
        AppendBucket(sb, "rss", rss, first: true);
        AppendBucket(sb, "cpu", cpu, first: false);
        AppendBucket(sb, "latency", latency, first: false);
        AppendBucket(sb, "bytes", bytes, first: false);
        AppendBucket(sb, "messages", messages, first: false);
        AppendStringField(sb, "window_start_ms", SoakFormat.Num(windowStartMs));
        AppendStringField(sb, "window_end_ms", SoakFormat.Num(windowEndMs));
        AppendStringField(sb, "measurement_start_ms", FormatMeasurementBound(Volatile.Read(ref _measurementStartMs)));
        AppendStringField(sb, "measurement_end_ms", FormatMeasurementBound(Volatile.Read(ref _measurementEndMs)));

        // The soak's own additions to the shared schema, in Python's order.
        AppendStringField(sb, "prefix", _prefix);

        sb.Append(", \"tags\": {");
        bool firstTag = true;
        foreach (KeyValuePair<string, string> tag in _baseTags.OrderBy(t => t.Key, StringComparer.Ordinal))
        {
            if (!firstTag)
            {
                sb.Append(", ");
            }

            firstTag = false;
            AppendJsonString(sb, tag.Key);
            sb.Append(": ");
            AppendJsonString(sb, tag.Value);
        }

        sb.Append('}');

        sb.Append(", \"counters\": {");
        bool firstCounter = true;
        foreach (KeyValuePair<string, (long Total, long Delta)> entry in counters.OrderBy(e => e.Key, StringComparer.Ordinal))
        {
            if (!firstCounter)
            {
                sb.Append(", ");
            }

            firstCounter = false;
            AppendJsonString(sb, entry.Key);
            sb.Append(": {\"total\": ").Append(SoakFormat.Num(entry.Value.Total))
              .Append(", \"delta\": ").Append(SoakFormat.Num(entry.Value.Delta)).Append('}');
        }

        sb.Append('}');

        sb.Append(", \"gauges\": {");
        bool firstGauge = true;
        foreach (KeyValuePair<string, BucketRollover> entry in gauges.OrderBy(e => e.Key, StringComparer.Ordinal))
        {
            if (!firstGauge)
            {
                sb.Append(", ");
            }

            firstGauge = false;
            AppendJsonString(sb, entry.Key);
            sb.Append(": {");
            AppendCoreBucketFields(sb, entry.Value);
            AppendPercentiles(sb, entry.Value);
            sb.Append('}');
        }

        sb.Append('}');
        sb.Append('}');
        return sb.ToString();
    }

    private static string FormatMeasurementBound(long value) =>
        value == UnsetMs ? SoakFormat.NegInf : SoakFormat.Num(value);

    private static void AppendBucket(StringBuilder sb, string key, BucketRollover b, bool first)
    {
        if (!first)
        {
            sb.Append(", ");
        }

        sb.Append('"').Append(key).Append("\": {");
        AppendCoreBucketFields(sb, b);
        AppendPercentiles(sb, b);
        sb.Append('}');
    }

    private static void AppendCoreBucketFields(StringBuilder sb, BucketRollover b)
    {
        sb.Append("\"average\": \"").Append(SoakFormat.Num(b.Average)).Append('"');
        sb.Append(", \"max\": \"").Append(SoakFormat.Num(b.Max)).Append('"');
        sb.Append(", \"total\": \"").Append(SoakFormat.Num(b.Total)).Append('"');
        sb.Append(", \"count\": \"").Append(SoakFormat.Num(b.Count)).Append('"');
    }

    private static void AppendPercentiles(StringBuilder sb, BucketRollover b)
    {
        if (!b.HasHistogram)
        {
            return;
        }

        sb.Append(", \"p50\": \"").Append(SoakFormat.Num(b.P50)).Append('"');
        sb.Append(", \"p90\": \"").Append(SoakFormat.Num(b.P90)).Append('"');
        sb.Append(", \"p99\": \"").Append(SoakFormat.Num(b.P99)).Append('"');
        sb.Append(", \"p999\": \"").Append(SoakFormat.Num(b.P999)).Append('"');
    }

    private static void AppendStringField(StringBuilder sb, string key, string value)
    {
        sb.Append(", \"").Append(key).Append("\": ");
        AppendJsonString(sb, value);
    }

    private static void AppendJsonString(StringBuilder sb, string value)
    {
        sb.Append('"');
        foreach (char c in value)
        {
            switch (c)
            {
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                case '\n':
                    sb.Append("\\n");
                    break;
                case '\r':
                    sb.Append("\\r");
                    break;
                case '\t':
                    sb.Append("\\t");
                    break;
                default:
                    if (c < 0x20)
                    {
                        sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        sb.Append(c);
                    }

                    break;
            }
        }

        sb.Append('"');
    }
}
