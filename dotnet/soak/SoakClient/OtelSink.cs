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
/// OpenTelemetry counters and gauges, mirroring <c>soakclient.py</c>'s instruments.
/// <para>
/// Built only by <see cref="Create"/>, which returns <see langword="null"/> — loudly —
/// unless a real, exporting SDK pipeline could be established. <b>Getting that wrong is
/// worse than having no telemetry at all.</b> The Python version's predecessor called
/// <c>get_meter(...)</c> with no <c>MeterProvider</c> installed, which returns a no-op
/// meter that silently discards every measurement while the startup line still said
/// "otel on"; confirmed against a real collector — 30+ minutes with
/// <c>OTEL_METRICS_EXPORTER=otlp</c>, counters flat, no errors logged. The .NET twin of
/// that bug is a <see cref="Meter"/> with no <see cref="MeterProvider"/> listening to it,
/// so this type never creates the <see cref="Meter"/> until the provider is built.
/// </para>
/// <para>
/// Thread-safe: <see cref="SoakMetrics"/> calls in from the producer loop, the consumer
/// loop, the delivery continuations and the main thread's resource sampling, and the SDK
/// collects from its own. One lock covers instrument creation (a check-then-set that
/// could otherwise register a duplicate instrument, which the SDK drops — silently losing
/// a series); the gauge store has its own.
/// </para>
/// <para>
/// ⚠ TWO PYTHON BRANCHES ARE DELIBERATELY DROPPED (PLAN D12), not reinvented:
/// </para>
/// <list type="number">
///   <item><description><c>_existing_real_provider()</c> — Python checks for a
///   <c>MeterProvider</c> already installed by <c>opentelemetry-instrument</c>, to avoid
///   double-reporting. .NET has no such wrapper in play: this sink builds its own
///   provider explicitly with <c>Sdk.CreateMeterProviderBuilder()</c> and owns it, and
///   nothing installs one before <c>Main</c> runs.</description></item>
///   <item><description>The grpc↔http exporter failover loop — Python fails over because
///   <i>which</i> exporter package is installed varies. In .NET both protocols ship in the
///   one <c>OpenTelemetry.Exporter.OpenTelemetryProtocol</c> package and are selected by
///   <c>OtlpExportProtocol</c>, so the protocol env vars are honoured and there is nothing
///   to fail over to.</description></item>
/// </list>
/// </summary>
internal sealed class OtelSink : ISoakTelemetrySink
{
    /// <summary>Meter / instrumentation-scope name, as it appears in the export.</summary>
    internal const string Scope = "confluent.kafka.soak.rust";

    /// <summary>
    /// Cap on the final flush. An unreachable collector otherwise retries for the SDK's
    /// default per-call timeout and stretches every shutdown, which eats into the
    /// shutdown watchdog's budget.
    /// </summary>
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

    /// <summary>
    /// Returns a working sink, or <see langword="null"/> with the reason logged. Never
    /// throws: telemetry must not be able to stop the soak.
    /// </summary>
    internal static OtelSink? Create(IReadOnlyDictionary<string, string> baseTags, SoakLogger logger)
    {
        IReadOnlyList<string> requested = RequestedExporters();
        if (requested.Count == 0)
        {
            // THE ONE BEHAVIOUR THAT MUST PORT EXACTLY (PLAN D12): unset or "none" means
            // no telemetry, and it must SAY SO and return null — never a Meter nothing
            // listens to. Note the plain .NET OTel SDK does not read OTEL_METRICS_EXPORTER
            // itself (that is an auto-instrumentation variable), so the soak reads it,
            // exactly as Python does.
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
                    // The SDK reads OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES itself; only
                    // supply a default service name when the operator did not set one, so an
                    // operator-provided name always wins.
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

    /// <summary>
    /// <c>OTEL_METRICS_EXPORTER</c> as a list, lower-cased and trimmed; empty when
    /// telemetry is off (unset, empty, or <c>none</c>).
    /// </summary>
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

    /// <summary>
    /// The OTLP transport, from the metrics-specific env var if set, otherwise the
    /// general one, otherwise gRPC (the spec's default).
    /// </summary>
    internal static OtlpExportProtocol ResolveProtocol()
    {
        string protocol = (SoakEnv.GetStringOrNull("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
            ?? SoakEnv.GetString("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")).Trim().ToLowerInvariant();
        return protocol.StartsWith("http", StringComparison.Ordinal)
            ? OtlpExportProtocol.HttpProtobuf
            : OtlpExportProtocol.Grpc;
    }

    /// <summary>The periodic export interval in ms (<c>OTEL_METRIC_EXPORT_INTERVAL</c>, default 60000).</summary>
    internal static int ExportIntervalMs() => SoakEnv.GetInt("OTEL_METRIC_EXPORT_INTERVAL", 60000);

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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

            // An OBSERVABLE gauge whose callback re-yields the retained last value for
            // every tag-set (see LastValueGauges): an event-driven series must not vanish
            // from the backend between updates.
            _meter.CreateObservableGauge(fullName, () => ObserveGauge(fullName), unit: null, description: fullName);
        }
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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
            // The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS / _CERTIFICATE
            // itself; do not second-guess it. Only the protocol is resolved here, because
            // .NET selects it by enum rather than by which package is installed.
            exporterOptions.Protocol = protocol;
            readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = intervalMs;
        });
    }

    private IEnumerable<Measurement<double>> ObserveGauge(string fullName)
    {
        // Snapshot under the store's lock (inside Snapshot), then yield outside it: a
        // generator holding a lock across yields would keep it for as long as the SDK
        // takes to consume.
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
