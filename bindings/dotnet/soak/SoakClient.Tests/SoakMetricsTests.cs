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
using System.Text.Json;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The metrics layer's two contracts a broker cannot verify for us: the record schema,
/// and the ms-recorded / seconds-exported split.
/// </summary>
public sealed class SoakMetricsTests
{
    private const string Prefix = "kafka.client.soak.rust_dotnet.";

    private sealed class CapturingSink : ISoakTelemetrySink
    {
        internal List<(string Name, double Value)> Gauges { get; } = new List<(string, double)>();

        internal List<(string Name, long Increment)> Counters { get; } = new List<(string, long)>();

        internal int ShutdownCount { get; private set; }

        public void IncrCounter(string fullName, long increment, IReadOnlyDictionary<string, string> tags) =>
            Counters.Add((fullName, increment));

        public void SetGauge(string fullName, double value, IReadOnlyDictionary<string, string> tags) =>
            Gauges.Add((fullName, value));

        public void Shutdown() => ShutdownCount++;

        public void Dispose()
        {
        }
    }

    private static SoakMetrics NewMetrics(StringWriter writer, ISoakTelemetrySink? sink) =>
        new SoakMetrics(writer, new Dictionary<string, string>(StringComparer.Ordinal) { ["host"] = "h" }, Prefix, sink);

    /// <summary>
    /// <c>SetGauge</c> records MILLISECONDS and exports SECONDS, in one place.
    /// <para>
    /// Guards the <c>SecondsOnExport</c> contract: callers hand <c>SetGauge</c>
    /// milliseconds and it converts on the way out, so the 1 ms-wide latency bucket keeps
    /// usable percentiles while dashboards still read seconds. It does NOT guard the call
    /// sites — a caller that divides by 1000 itself reaches <c>SetGauge</c> with seconds,
    /// lands every sample in bucket 0 and reports the percentiles as zero, which is
    /// exactly what happened before this contract existed. The defence there is the
    /// comment at the call site.
    /// </para>
    /// </summary>
    [Fact]
    public void LatencyGaugesRecordMsAndExportSeconds()
    {
        using var writer = new StringWriter();
        var sink = new CapturingSink();
        using SoakMetrics metrics = NewMetrics(writer, sink);

        // 17 ms, the order of magnitude a healthy soak actually reports.
        var partition = new Dictionary<string, string>(StringComparer.Ordinal) { ["partition"] = "0" };
        metrics.SetGauge("producer.latency", 17.0, partition);
        metrics.SetGauge("consumer.e2e_latency", 17.0, partition);

        // Not in SecondsOnExport: its name asserts milliseconds.
        metrics.SetGauge("consumer.recovery_ms", 17.0);

        var exported = new Dictionary<string, double>(StringComparer.Ordinal);
        foreach ((string name, double value) in sink.Gauges)
        {
            exported[name] = value;
        }

        Assert.Equal(0.017, exported[Prefix + "producer.latency"], 6);
        Assert.Equal(0.017, exported[Prefix + "consumer.e2e_latency"], 6);
        Assert.Equal(17.0, exported[Prefix + "consumer.recovery_ms"], 6);

        // The histogram kept milliseconds, so the percentiles are non-zero.
        using JsonDocument record = JsonDocument.Parse(metrics.Rollover());
        JsonElement gauges = record.RootElement.GetProperty("gauges");
        int latencySeriesChecked = 0;
        foreach (JsonProperty gauge in gauges.EnumerateObject())
        {
            if (!gauge.Name.Contains("latency", StringComparison.Ordinal)
                || !gauge.Value.TryGetProperty("p99", out JsonElement p99))
            {
                continue;
            }

            latencySeriesChecked++;
            Assert.True(
                double.Parse(p99.GetString()!, CultureInfo.InvariantCulture) > 0,
                gauge.Name + " p99 collapsed to zero: the histogram was fed seconds");
        }

        Assert.Equal(2, latencySeriesChecked);
    }

    [Fact]
    public void CountersCarryTotalAndDeltaAgainstTheLastRollover()
    {
        using var writer = new StringWriter();
        using SoakMetrics metrics = NewMetrics(writer, null);

        metrics.IncrCounter("consumer.msgdup", 3);
        using (JsonDocument first = JsonDocument.Parse(metrics.Rollover()))
        {
            JsonElement counter = first.RootElement.GetProperty("counters").GetProperty("consumer.msgdup");
            Assert.Equal(3, counter.GetProperty("total").GetInt64());
            Assert.Equal(3, counter.GetProperty("delta").GetInt64());
        }

        metrics.IncrCounter("consumer.msgdup", 5);
        using JsonDocument second = JsonDocument.Parse(metrics.Rollover());
        JsonElement rolled = second.RootElement.GetProperty("counters").GetProperty("consumer.msgdup");
        Assert.Equal(8, rolled.GetProperty("total").GetInt64());
        Assert.Equal(5, rolled.GetProperty("delta").GetInt64());
    }

    /// <summary>
    /// The record schema must stay identical to the perf harness's — that is what makes
    /// soak and perf numbers comparable across clients. Pin the blocks and the window
    /// bounds so a refactor cannot quietly rename one.
    /// </summary>
    [Fact]
    public void RecordCarriesTheSharedSchemaPlusTheSoakAdditions()
    {
        using var writer = new StringWriter();
        using SoakMetrics metrics = NewMetrics(writer, null);
        metrics.ObserveMessage(1234, 7.0);

        using JsonDocument record = JsonDocument.Parse(metrics.Rollover());
        JsonElement root = record.RootElement;

        foreach (string block in new[] { "rss", "cpu", "latency", "bytes", "messages" })
        {
            JsonElement element = root.GetProperty(block);
            foreach (string field in new[] { "average", "max", "total", "count" })
            {
                Assert.Equal(JsonValueKind.String, element.GetProperty(field).ValueKind);
            }
        }

        // Only the latency block carries percentiles (the perf harness's shape).
        foreach (string percentile in new[] { "p50", "p90", "p99", "p999" })
        {
            Assert.True(root.GetProperty("latency").TryGetProperty(percentile, out _));
            Assert.False(root.GetProperty("bytes").TryGetProperty(percentile, out _));
        }

        foreach (string field in new[] { "window_start_ms", "window_end_ms", "measurement_start_ms", "measurement_end_ms" })
        {
            Assert.Equal(JsonValueKind.String, root.GetProperty(field).ValueKind);
        }

        // An unset measurement bound is the "-inf" sentinel the plot tool special-cases.
        Assert.Equal("-inf", root.GetProperty("measurement_start_ms").GetString());

        Assert.Equal(Prefix, root.GetProperty("prefix").GetString());
        Assert.Equal("h", root.GetProperty("tags").GetProperty("host").GetString());
        Assert.Equal("1234", root.GetProperty("bytes").GetProperty("total").GetString());
        Assert.Equal("1", root.GetProperty("messages").GetProperty("total").GetString());
    }

    [Fact]
    public void MeasurementBoundsAreEmittedOnceSet()
    {
        using var writer = new StringWriter();
        using SoakMetrics metrics = NewMetrics(writer, null);
        metrics.SetMeasurementStart(1765432100123);

        using JsonDocument record = JsonDocument.Parse(metrics.Rollover());
        Assert.Equal("1765432100123", record.RootElement.GetProperty("measurement_start_ms").GetString());
        Assert.Equal("-inf", record.RootElement.GetProperty("measurement_end_ms").GetString());
    }

    /// <summary>
    /// Tag values are operator-chosen (a variant label, a topic), so a stray quote must
    /// not make the whole two-week series unparseable.
    /// </summary>
    [Fact]
    public void JsonStringsAreEscaped()
    {
        using var writer = new StringWriter();
        var baseTags = new Dictionary<string, string>(StringComparer.Ordinal) { ["variant"] = "a\"b\\c" };
        using var metrics = new SoakMetrics(writer, baseTags, Prefix, null);
        metrics.IncrCounter("weird", 1, new Dictionary<string, string>(StringComparer.Ordinal) { ["k"] = "\"quoted\"" });

        using JsonDocument record = JsonDocument.Parse(metrics.Rollover());
        Assert.Equal("a\"b\\c", record.RootElement.GetProperty("tags").GetProperty("variant").GetString());
        Assert.True(record.RootElement.GetProperty("counters").TryGetProperty("weird{k=\"quoted\"}", out _));
    }

    [Fact]
    public void MetricKeyRendersTagsSortedAndStable()
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["b"] = "2", ["a"] = "1" };
        Assert.Equal("m{a=1,b=2}", SoakMetrics.MetricKey("m", tags));
        Assert.Equal("m", SoakMetrics.MetricKey("m", null));
        Assert.Equal("m", SoakMetrics.MetricKey("m", new Dictionary<string, string>(StringComparer.Ordinal)));
    }

    /// <summary>The file is APPENDED, never truncated: run.sh restarts the client repeatedly.</summary>
    [Fact]
    public void WritesAppendToTheMetricsFile()
    {
        string path = Path.Combine(Path.GetTempPath(), "soak-metrics-test-" + Guid.NewGuid().ToString("N") + ".jsonl");
        try
        {
            File.WriteAllText(path, "{\"pre-existing\": true}\n");

            var baseTags = new Dictionary<string, string>(StringComparer.Ordinal);
            using (var metrics = new SoakMetrics(path, baseTags, new SoakLogger(SoakLogLevel.Fatal), Prefix, null))
            {
                metrics.WriteFinal();
            }

            string[] lines = File.ReadAllLines(path);
            Assert.Equal(2, lines.Length);
            Assert.Contains("pre-existing", lines[0], StringComparison.Ordinal);
            using JsonDocument appended = JsonDocument.Parse(lines[1]);
            Assert.Equal(Prefix, appended.RootElement.GetProperty("prefix").GetString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void CloseShutsTheTelemetryPipelineDownOnce()
    {
        using var writer = new StringWriter();
        var sink = new CapturingSink();
        var metrics = NewMetrics(writer, sink);

        Assert.True(metrics.OtelEnabled);
        metrics.Close();
        metrics.Close();
        metrics.Dispose();

        Assert.Equal(1, sink.ShutdownCount);
        Assert.False(metrics.OtelEnabled);
    }

    /// <summary>A writer whose <c>WriteLine</c> always fails — a full disk, in effect.</summary>
    private sealed class ThrowingWriter : StringWriter
    {
        private int _attempts;

        internal int Attempts => System.Threading.Volatile.Read(ref _attempts);

        public override void WriteLine(string? value)
        {
            System.Threading.Interlocked.Increment(ref _attempts);
            throw new IOException("no space left on device");
        }
    }

    /// <summary>
    /// ⚠ REGRESSION GUARD. An unhandled exception on a background thread terminates the
    /// whole .NET process, so an unguarded rollover loop would let a full disk — entirely
    /// plausible on a two-week run — kill the soak from its own telemetry. The loop must
    /// log the window and keep sampling.
    /// <para>
    /// Without the guard this test does not merely fail, it takes the test HOST down, so
    /// <c>Attempts &gt;= 2</c> is the real assertion: the collector tried again after the
    /// first failure rather than dying on it.
    /// </para>
    /// </summary>
    [Fact]
    public void CollectorSurvivesAWriteFailure()
    {
        using var writer = new ThrowingWriter();
        using var metrics = new SoakMetrics(
            writer,
            new Dictionary<string, string>(StringComparer.Ordinal),
            Prefix,
            null,
            new SoakLogger(SoakLogLevel.Fatal));

        metrics.StartCollecting(0.01);
        for (int i = 0; i < 100 && writer.Attempts < 2; i++)
        {
            System.Threading.Thread.Sleep(20);
        }

        metrics.StopCollecting();
        Assert.True(writer.Attempts >= 2, "the collector stopped after its first write failure (attempts: " + writer.Attempts + ")");
    }

    [Fact]
    public void LatencyGaugeMembershipMatchesThePythonSets()
    {
        Assert.Contains("producer.latency", SoakMetrics.LatencyGauges);
        Assert.Contains("consumer.e2e_latency", SoakMetrics.LatencyGauges);
        Assert.Contains("consumer.recovery_ms", SoakMetrics.LatencyGauges);

        Assert.Contains("producer.latency", SoakMetrics.SecondsOnExport);
        Assert.Contains("consumer.e2e_latency", SoakMetrics.SecondsOnExport);

        // Deliberately absent: its name asserts milliseconds and it has no
        // reference-soak counterpart to match.
        Assert.DoesNotContain("consumer.recovery_ms", SoakMetrics.SecondsOnExport);
    }
}
