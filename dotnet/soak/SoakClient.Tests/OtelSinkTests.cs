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
using OpenTelemetry.Exporter;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// Environment variables are process-global, so every test that sets one runs in this
/// collection, serialized against the others.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class EnvironmentCollection
{
    /// <summary>The collection name.</summary>
    public const string Name = "environment";
}

/// <summary>
/// The telemetry pipeline's honest-availability reporting: telemetry that is requested
/// but unbuildable must be loud, and telemetry that is not requested must be null — never
/// a meter nothing listens to.
/// </summary>
[Collection(EnvironmentCollection.Name)]
public sealed class OtelSinkTests : IDisposable
{
    private static readonly string[] s_variables =
    {
        "OTEL_METRICS_EXPORTER",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        "OTEL_METRIC_EXPORT_INTERVAL",
    };

    private readonly Dictionary<string, string?> _saved = new Dictionary<string, string?>(StringComparer.Ordinal);

    /// <summary>Snapshots the OTEL variables so each test restores the ambient environment.</summary>
    public OtelSinkTests()
    {
        foreach (string name in s_variables)
        {
            _saved[name] = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        foreach (KeyValuePair<string, string?> entry in _saved)
        {
            Environment.SetEnvironmentVariable(entry.Key, entry.Value);
        }
    }

    [Theory]
    [InlineData("", new string[0])]
    [InlineData("none", new string[0])]
    [InlineData("NONE", new string[0])]
    [InlineData("otlp", new[] { "otlp" })]
    [InlineData("OTLP", new[] { "otlp" })]
    [InlineData("otlp,console", new[] { "otlp", "console" })]
    [InlineData(" otlp , console ", new[] { "otlp", "console" })]
    public void RequestedExportersParsing(string value, string[] expected)
    {
        Environment.SetEnvironmentVariable("OTEL_METRICS_EXPORTER", value);
        Assert.Equal(expected, OtelSink.RequestedExporters());
    }

    [Fact]
    public void RequestedExportersWhenUnset()
    {
        Environment.SetEnvironmentVariable("OTEL_METRICS_EXPORTER", null);
        Assert.Empty(OtelSink.RequestedExporters());
    }

    /// <summary>
    /// ⚠ THE ONE BEHAVIOUR THAT MUST PORT EXACTLY (PLAN D12): no
    /// <c>OTEL_METRICS_EXPORTER</c> means no telemetry, and <c>Create</c> must return
    /// null rather than a <c>Meter</c> with no <c>MeterProvider</c> listening — which
    /// would silently discard every measurement while the startup line claimed "otel on".
    /// </summary>
    [Fact]
    public void CreateReturnsNullWhenNoExporterIsRequested()
    {
        Environment.SetEnvironmentVariable("OTEL_METRICS_EXPORTER", null);
        Assert.Null(OtelSink.Create(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["host"] = "h" },
            new SoakLogger(SoakLogLevel.Fatal)));
    }

    [Fact]
    public void CreateReturnsNullWhenTheExporterIsNone()
    {
        Environment.SetEnvironmentVariable("OTEL_METRICS_EXPORTER", "none");
        Assert.Null(OtelSink.Create(
            new Dictionary<string, string>(StringComparer.Ordinal),
            new SoakLogger(SoakLogLevel.Fatal)));
    }

    /// <summary>
    /// An unsupported exporter name must DISABLE telemetry with a logged reason, never
    /// throw: telemetry must not be able to stop the soak.
    /// </summary>
    [Fact]
    public void CreateReturnsNullForAnUnsupportedExporter()
    {
        Environment.SetEnvironmentVariable("OTEL_METRICS_EXPORTER", "carrier-pigeon");
        Assert.Null(OtelSink.Create(
            new Dictionary<string, string>(StringComparer.Ordinal),
            new SoakLogger(SoakLogLevel.Fatal)));
    }

    [Theory]
    [InlineData(null, null, "Grpc")]
    [InlineData("grpc", null, "Grpc")]
    [InlineData("http/protobuf", null, "HttpProtobuf")]
    [InlineData("HTTP/PROTOBUF", null, "HttpProtobuf")]
    // The metrics-specific variable wins over the general one.
    [InlineData("grpc", "http/protobuf", "HttpProtobuf")]
    [InlineData("http/protobuf", "grpc", "Grpc")]
    public void ResolveProtocolHonoursTheSpecEnvVars(string? general, string? metrics, string expected)
    {
        Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_PROTOCOL", general);
        Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", metrics);

        OtlpExportProtocol resolved = OtelSink.ResolveProtocol();
        Assert.Equal(expected, resolved.ToString());
    }

    [Fact]
    public void ExportIntervalDefaultsToSixtySeconds()
    {
        Environment.SetEnvironmentVariable("OTEL_METRIC_EXPORT_INTERVAL", null);
        Assert.Equal(60000, OtelSink.ExportIntervalMs());

        Environment.SetEnvironmentVariable("OTEL_METRIC_EXPORT_INTERVAL", "15000");
        Assert.Equal(15000, OtelSink.ExportIntervalMs());
    }

    /// <summary>
    /// The console exporter builds without a collector, so this is the one place the full
    /// pipeline — provider, meter, counter, observable gauge, shutdown — is exercised end
    /// to end with no network.
    /// </summary>
    [Fact]
    public void CreateBuildsAWorkingPipelineForTheConsoleExporter()
    {
        Environment.SetEnvironmentVariable("OTEL_METRICS_EXPORTER", "console");
        Environment.SetEnvironmentVariable("OTEL_METRIC_EXPORT_INTERVAL", "3600000");

        OtelSink? sink = OtelSink.Create(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["host"] = "h" },
            new SoakLogger(SoakLogLevel.Fatal));

        Assert.NotNull(sink);
        using (sink)
        {
            var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["partition"] = "0" };
            sink!.IncrCounter("kafka.client.soak.rust_dotnet.producer.send", 1, tags);
            sink.IncrCounter("kafka.client.soak.rust_dotnet.producer.send", 1, tags);
            sink.SetGauge("kafka.client.soak.rust_dotnet.consumer.e2e_latency", 0.017, tags);

            // Registering the same gauge twice must not register a duplicate instrument
            // (the SDK drops one, silently losing a series).
            sink.SetGauge("kafka.client.soak.rust_dotnet.consumer.e2e_latency", 0.018, tags);
            sink.Shutdown();
        }
    }
}
