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
/// The telemetry pipeline <see cref="SoakMetrics"/> drives <i>in addition</i> to the
/// JSONL file. Injectable so the export-path contracts (notably the ms-recorded /
/// seconds-exported conversion) can be asserted without a collector.
/// </summary>
internal interface ISoakTelemetrySink : IDisposable
{
    /// <summary>Adds <paramref name="increment"/> to the counter <paramref name="fullName"/>.</summary>
    void IncrCounter(string fullName, long increment, IReadOnlyDictionary<string, string> tags);

    /// <summary>Records the latest value of the gauge <paramref name="fullName"/>.</summary>
    void SetGauge(string fullName, double value, IReadOnlyDictionary<string, string> tags);

    /// <summary>Flushes and stops the pipeline. Never throws, never blocks indefinitely.</summary>
    void Shutdown();
}

/// <summary>
/// The soak's counters and gauges on top of the forked metric primitives
/// (<see cref="Bucket"/> / <see cref="LatencyHistogram"/>), plus the per-window rollover
/// thread that appends one JSON object per line to the metrics file.
/// <para>
/// The JSONL file is <b>always</b> written — it is the durable local record a two-week
/// run is analysed from, and it is what makes the soak runnable whether or not the OTLP
/// pipeline is up. An OpenTelemetry pipeline is driven in addition when
/// <c>OTEL_METRICS_EXPORTER</c> requests one <i>and it could actually be built</i>; see
/// <see cref="OtelSink.Create"/>, which logs loudly and falls back to JSONL rather than
/// silently exporting into a meter nothing listens to.
/// </para>
/// <para>
/// The file is <b>appended</b>, not truncated (<c>run.sh</c> restarts the client
/// repeatedly and each restart must extend the series), and flushed on every write so a
/// <c>kill -9</c> or a log-rotating supervisor cannot lose samples already collected.
/// </para>
/// </summary>
internal sealed class SoakMetrics : IDisposable
{
    /// <summary>Sentinel for an unset measurement bound — serialized as the <c>"-inf"</c> string.</summary>
    private const long UnsetMs = long.MinValue;

    /// <summary>Gauges recorded through the 1 ms histogram rather than a plain bucket.</summary>
    internal static readonly IReadOnlyCollection<string> LatencyGauges = new HashSet<string>(StringComparer.Ordinal)
    {
        "producer.latency",
        "consumer.e2e_latency",
        "consumer.recovery_ms",
    };

    /// <summary>
    /// Gauges EXPORTED in seconds while still being RECORDED in milliseconds.
    /// <para>
    /// The reference soak reports both of these as a float in seconds, so dashboards
    /// built against <c>kafka.client.soak.python.*</c> read ours on the same scale. The
    /// conversion has to happen on the export path and nowhere else: the latency buckets
    /// are 1 ms wide, so handing one seconds sends every sample to <c>(long)0.016 == 0</c>
    /// and reports p50/p90/p99/p999 as zero — silently destroying the JSONL, which is the
    /// artifact a two-week run is actually analysed from.
    /// </para>
    /// <para>
    /// <c>consumer.recovery_ms</c> is deliberately absent: its name asserts milliseconds
    /// and it has no reference-soak counterpart to match.
    /// </para>
    /// </summary>
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

    // The collector waits on this rather than sleeping, so Stop returns promptly instead
    // of waiting out the current window. At the soak's 10 s interval a Thread.Sleep-based
    // sampler would stall every shutdown by up to 10 s.
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

    /// <summary>
    /// Creates the metrics harness, opening <paramref name="path"/> in APPEND mode.
    /// </summary>
    internal SoakMetrics(
        string path,
        IReadOnlyDictionary<string, string> baseTags,
        SoakLogger logger,
        string prefix,
        ISoakTelemetrySink? sink)
        : this(new StreamWriter(path, append: true), baseTags, prefix, sink, logger)
    {
    }

    /// <summary>Creates the harness over an arbitrary writer (used by the unit tests).</summary>
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

    /// <summary>Whether a real exporting telemetry pipeline was established.</summary>
    internal bool OtelEnabled => _otel is not null;

    /// <summary>The metric-name prefix every exported instrument carries.</summary>
    internal string Prefix => _prefix;

    /// <summary>Wall-clock epoch milliseconds (Python's <c>int(time.time() * 1000)</c>).</summary>
    internal static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>Marks the start of the measured interval; windows from here feed the CPU/RSS averages.</summary>
    internal void SetMeasurementStart(long epochMs) => Volatile.Write(ref _measurementStartMs, epochMs);

    /// <summary>Marks the end of the measured interval; later windows are cooldown and excluded.</summary>
    internal void SetMeasurementEnd(long epochMs) => Volatile.Write(ref _measurementEndMs, epochMs);

    /// <summary>Increments a counter, both in the JSONL record and (if enabled) on the export pipeline.</summary>
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

    /// <summary>
    /// Records a gauge observation. Callers hand this MILLISECONDS for everything in
    /// <see cref="SecondsOnExport"/>; the seconds conversion happens here, on the export
    /// path only, so the 1 ms-wide histogram keeps usable percentiles.
    /// </summary>
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

    /// <summary>Feeds the throughput / latency buckets for one consumed message.</summary>
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

    /// <summary>
    /// Starts the background rollover thread. Idempotent. The thread is a background
    /// thread so a crash before <see cref="StopCollecting"/> still lets the process exit
    /// (mirroring Python's <c>daemon=True</c>); a normal shutdown joins it.
    /// </summary>
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
                // ⚠ TOTAL BY CONSTRUCTION. An unhandled exception on a background thread
                // terminates the whole .NET process, so without this guard a full disk
                // (entirely plausible on a two-week run) or a transient Process.Refresh
                // failure would kill the soak from its own telemetry. Log the window and
                // keep sampling: losing one window is a data gap, losing the process is
                // the run.
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

    /// <summary>Stops the rollover thread and joins it.</summary>
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

    /// <summary>Writes one last window before shutting down.</summary>
    internal void WriteFinal() => WriteRecord(Rollover());

    /// <summary>Flushes and stops the telemetry pipeline, then closes the metrics file.</summary>
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

    /// <inheritdoc/>
    public void Dispose()
    {
        StopCollecting();
        _stop.Dispose();
        Close();
    }

    /// <summary>
    /// Rolls every window accumulator over and returns the JSON line. Internal rather than
    /// private so the tests can assert the ms-recorded / seconds-exported contract on the
    /// resulting percentiles.
    /// </summary>
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

    /// <summary>
    /// The JSONL key for a metric: the bare name, or <c>name{k=v,k2=v2}</c> with the tags
    /// sorted, matching Python's <c>SoakMetrics._key</c>.
    /// </summary>
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

    /// <summary>
    /// Appends a JSON string literal. Counter/gauge keys are built from metric names and
    /// tag values — a topic or variant an operator chose — so they are escaped rather than
    /// interpolated raw: one stray quote would otherwise make the whole two-week series
    /// unparseable.
    /// </summary>
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
