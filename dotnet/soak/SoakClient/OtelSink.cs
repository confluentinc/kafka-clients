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
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Confluent.Kafka.Soak;

/// <summary>
/// OpenTelemetry counters and gauges, as Python's soak instruments; built only by
/// <see cref="Create"/>, which creates the <see cref="Meter"/> only once an exporting provider
/// exists, never a meter nothing listens to.
/// </summary>
internal sealed class OtelSink : ISoakTelemetrySink
{
    internal const string Scope = "confluent.kafka.soak.rust";

    // Caps the final flush, which an unreachable collector would otherwise stretch into the
    // shutdown watchdog's budget.
    internal const int ShutdownTimeoutMs = 5000;

    private readonly object _lock = new object();
    private readonly Dictionary<string, Counter<long>> _counters = new Dictionary<string, Counter<long>>(StringComparer.Ordinal);
    private readonly HashSet<string> _gauges = new HashSet<string>(StringComparer.Ordinal);
    private readonly LastValueGauges _gaugeValues = new LastValueGauges();
    private readonly IReadOnlyDictionary<string, string> _baseTags;
    private readonly MeterProvider _provider;
    private readonly Meter _meter;
    private bool _disposed;

    private OtelSink(MeterProvider provider, Meter meter, IReadOnlyDictionary<string, string> baseTags)
    {
        _provider = provider;
        _meter = meter;
        _baseTags = baseTags;
    }

    // Never throws. Unlike Python, no check for an installed provider (none is installed here) and
    // no grpc/http failover (one package carries both).
    internal static OtelSink? Create(IReadOnlyDictionary<string, string> baseTags, SoakLogger logger)
    {
        IReadOnlyList<string> requested = RequestedExporters();
        if (requested.Count == 0)
        {
            // The soak reads OTEL_METRICS_EXPORTER itself, as Python (the .NET SDK does not), and
            // says so when it turns telemetry off.
            logger.Info("telemetry: OTEL_METRICS_EXPORTER is unset or 'none'; metrics go to the JSONL file only");
            return null;
        }

        MeterProvider? provider = null;
        try
        {
            int intervalMs = ExportIntervalMs();
            MeterProviderBuilder builder = Sdk.CreateMeterProviderBuilder()
                .AddMeter(Scope)
                .ConfigureResource(resource =>
                {
                    if (SoakEnv.GetStringOrNull("OTEL_SERVICE_NAME") is null)
                    {
                        resource.AddService("kafka-client-soak-rust");
                    }
                });

            foreach (string name in requested)
            {
                builder = AddExporter(builder, name, intervalMs);
            }

            provider = builder.Build();
            var meter = new Meter(Scope);
            var sink = new OtelSink(provider, meter, baseTags);

            logger.Info(string.Format(
                CultureInfo.InvariantCulture,
                "telemetry: OTLP pipeline installed (exporters={0}, interval={1}ms, endpoint={2}, scope={3})",
                string.Join(",", requested),
                intervalMs,
                SoakEnv.GetStringOrNull("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
                    ?? SoakEnv.GetString("OTEL_EXPORTER_OTLP_ENDPOINT", "<sdk default>"),
                Scope));
            return sink;
        }
        catch (Exception ex)
        {
            provider?.Dispose();
            logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "telemetry: DISABLED — could not build the {0} exporter pipeline: {1}. Metrics go to the JSONL file only.",
                string.Join(",", requested),
                ex.Message));
            return null;
        }
    }

    internal static IReadOnlyList<string> RequestedExporters()
    {
        string raw = SoakEnv.GetString("OTEL_METRICS_EXPORTER", string.Empty).Trim();
        if (raw.Length == 0 || string.Equals(raw, "none", StringComparison.OrdinalIgnoreCase))
        {
            return Array.Empty<string>();
        }

        return raw.Split(',')
            .Select(name => name.Trim().ToLowerInvariant())
            .Where(name => name.Length > 0)
            .ToList();
    }

    internal static OtlpExportProtocol ResolveProtocol()
    {
        string protocol = (SoakEnv.GetStringOrNull("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
            ?? SoakEnv.GetString("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")).Trim().ToLowerInvariant();
        return protocol.StartsWith("http", StringComparison.Ordinal)
            ? OtlpExportProtocol.HttpProtobuf
            : OtlpExportProtocol.Grpc;
    }

    internal static int ExportIntervalMs() => SoakEnv.GetInt("OTEL_METRIC_EXPORT_INTERVAL", 60000);

    public void IncrCounter(string fullName, long increment, IReadOnlyDictionary<string, string> tags)
    {
        Counter<long>? counter;
        lock (_lock)
        {
            if (_disposed)
            {
                return;
            }

            if (!_counters.TryGetValue(fullName, out counter))
            {
                counter = _meter.CreateCounter<long>(fullName, unit: null, description: fullName);
                _counters[fullName] = counter;
            }
        }

        counter.Add(increment, Merge(tags));
    }

    public void SetGauge(string fullName, double value, IReadOnlyDictionary<string, string> tags)
    {
        IReadOnlyDictionary<string, string> merged = MergeDictionary(tags);
        _gaugeValues.Record(fullName, value, merged);

        lock (_lock)
        {
            if (_disposed || !_gauges.Add(fullName))
            {
                return;
            }

            _meter.CreateObservableGauge(fullName, () => ObserveGauge(fullName), unit: null, description: fullName);
        }
    }

    public void Shutdown()
    {
        // Best-effort: a collector that is down at shutdown must not turn a clean exit
        // into a hang or a stack trace.
        try
        {
            _provider.ForceFlush(ShutdownTimeoutMs);
        }
        catch (Exception)
        {
            // Ignored — see above.
        }

        try
        {
            _provider.Shutdown(ShutdownTimeoutMs);
        }
        catch (Exception)
        {
            // Ignored — see above.
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        _meter.Dispose();
        _provider.Dispose();
    }

    private static MeterProviderBuilder AddExporter(MeterProviderBuilder builder, string name, int intervalMs)
    {
        if (string.Equals(name, "console", StringComparison.Ordinal))
        {
            return builder.AddConsoleExporter((_, readerOptions) =>
                readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = intervalMs);
        }

        if (!string.Equals(name, "otlp", StringComparison.Ordinal))
        {
            throw new ArgumentException(string.Format(
                CultureInfo.InvariantCulture,
                "unsupported OTEL_METRICS_EXPORTER '{0}' (supported: otlp, console, none)",
                name));
        }

        OtlpExportProtocol protocol = ResolveProtocol();
        return builder.AddOtlpExporter((exporterOptions, readerOptions) =>
        {
            exporterOptions.Protocol = protocol;
            readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = intervalMs;
        });
    }

    private IEnumerable<Measurement<double>> ObserveGauge(string fullName)
    {
        // Snapshot under the store's lock and yield outside it, so the SDK never holds the lock.
        foreach ((double value, IReadOnlyDictionary<string, string> tags) in _gaugeValues.Snapshot(fullName))
        {
            yield return new Measurement<double>(value, ToTagArray(tags));
        }
    }

    private IReadOnlyDictionary<string, string> MergeDictionary(IReadOnlyDictionary<string, string> tags)
    {
        var merged = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in tags)
        {
            merged[tag.Key] = tag.Value;
        }

        // Base tags win, as in Python (`merged.update(self._base_tags)`).
        foreach (KeyValuePair<string, string> tag in _baseTags)
        {
            merged[tag.Key] = tag.Value;
        }

        return merged;
    }

    private KeyValuePair<string, object?>[] Merge(IReadOnlyDictionary<string, string> tags) =>
        ToTagArray(MergeDictionary(tags));

    private static KeyValuePair<string, object?>[] ToTagArray(IReadOnlyDictionary<string, string> tags)
    {
        var array = new KeyValuePair<string, object?>[tags.Count];
        int index = 0;
        foreach (KeyValuePair<string, string> tag in tags)
        {
            array[index++] = new KeyValuePair<string, object?>(tag.Key, tag.Value);
        }

        return array;
    }
}
