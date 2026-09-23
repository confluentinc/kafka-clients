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
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The <c>SOAK_*</c> environment contract <c>run.sh</c> exports into. Defaults must match
/// the Python soak's <c>argparse</c> declarations, because the profiles
/// (<c>848-normal</c> / <c>848-hi-throughput</c>) were tuned against those numbers.
/// </summary>
[Collection(EnvironmentCollection.Name)]
public sealed class SoakOptionsTests : IDisposable
{
    private static readonly string[] s_variables =
    {
        "SOAK_TESTID", "SOAK_TOPIC", "SOAK_RATE", "SOAK_VARIANT", "SOAK_CONFIG_FILE",
        "SOAK_METRICS_FILE", "SOAK_BROKERS", "SOAK_PAYLOAD_SIZE", "SOAK_PARTITIONS",
        "SOAK_REPLICATION_FACTOR", "SOAK_RECREATE_TOPIC", "SOAK_METRICS_INTERVAL",
        "SOAK_COMMIT_INTERVAL", "SOAK_POLL_TIMEOUT", "SOAK_STALL_THRESHOLD",
        "SOAK_MAX_SEND_ATTEMPTS", "SOAK_MAX_POLL_FAILURES", "SOAK_RUNTIME_SECONDS",
        "SOAK_SHUTDOWN_TIMEOUT", "SOAK_LOG_LEVEL",
    };

    private readonly Dictionary<string, string?> _saved = new Dictionary<string, string?>(StringComparer.Ordinal);

    /// <summary>Snapshots and clears the SOAK variables so each test starts from a known state.</summary>
    public SoakOptionsTests()
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

    [Fact]
    public void DefaultsMatchThePythonSoak()
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "soak1");
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "rustsoak-soak1");

        SoakOptions options = SoakOptions.FromEnvironment();

        Assert.Equal("soak1", options.TestId);
        Assert.Equal("rustsoak-soak1", options.Topic);
        Assert.Equal("unspecified", options.Variant);
        Assert.Equal(80.0, options.Rate);
        Assert.Equal(50, options.PayloadSize);
        Assert.Equal(2, options.Partitions);
        Assert.Equal(-1, options.ReplicationFactor);
        Assert.False(options.RecreateTopic);
        Assert.Equal(10.0, options.MetricsIntervalSeconds);
        Assert.Equal(5.0, options.CommitIntervalSeconds);
        Assert.Equal(1.0, options.PollTimeoutSeconds);
        Assert.Equal(5.0, options.StallThresholdSeconds);
        Assert.Equal(10, options.MaxSendAttempts);
        Assert.Equal(20, options.MaxPollFailures);
        Assert.Equal(0.0, options.RuntimeSeconds);
        Assert.Equal(60.0, options.ShutdownTimeoutSeconds);
        Assert.Equal(SoakLogLevel.Info, options.LogLevel);
        Assert.Null(options.ConfigFile);
        Assert.Null(options.Brokers);
        Assert.Equal("soak-metrics-unspecified-soak1.jsonl", options.MetricsFile);
    }

    [Fact]
    public void MetricsFileNameFollowsTheVariantAndTestId()
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "s2");
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "t");
        Environment.SetEnvironmentVariable("SOAK_VARIANT", "848-hi-throughput");

        Assert.Equal("soak-metrics-848-hi-throughput-s2.jsonl", SoakOptions.FromEnvironment().MetricsFile);
    }

    [Fact]
    public void HiProfileValuesAreAccepted()
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "s3");
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "t");
        Environment.SetEnvironmentVariable("SOAK_RATE", "1000");
        Environment.SetEnvironmentVariable("SOAK_PAYLOAD_SIZE", "10240");

        SoakOptions options = SoakOptions.FromEnvironment();
        Assert.Equal(1000.0, options.Rate);
        Assert.Equal(10240, options.PayloadSize);
    }

    [Fact]
    public void MissingTestIdIsRejected()
    {
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "t");
        ArgumentException ex = Assert.Throws<ArgumentException>(SoakOptions.FromEnvironment);
        Assert.Contains("SOAK_TESTID", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MissingTopicIsRejected()
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "s");
        ArgumentException ex = Assert.Throws<ArgumentException>(SoakOptions.FromEnvironment);
        Assert.Contains("SOAK_TOPIC", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    public void NonPositiveRateIsRejected(string rate)
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "s");
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "t");
        Environment.SetEnvironmentVariable("SOAK_RATE", rate);

        ArgumentException ex = Assert.Throws<ArgumentException>(SoakOptions.FromEnvironment);
        Assert.Contains("SOAK_RATE must be greater than zero", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Booleans follow the perf suite's case-sensitive <c>"True"</c> convention (Python's
    /// <c>os.getenv(name, default) == "True"</c>), so <c>true</c> / <c>1</c> are NOT
    /// truthy — a mismatch here would silently make a destructive flag a no-op, or worse,
    /// silently enable one.
    /// </summary>
    [Theory]
    [InlineData("True", true)]
    [InlineData("true", false)]
    [InlineData("1", false)]
    [InlineData("yes", false)]
    public void RecreateTopicUsesTheCaseSensitiveTrueConvention(string value, bool expected)
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "s");
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "t");
        Environment.SetEnvironmentVariable("SOAK_RECREATE_TOPIC", value);

        Assert.Equal(expected, SoakOptions.FromEnvironment().RecreateTopic);
    }

    // The expected level is passed as its NAME rather than as the enum: SoakLogLevel is
    // internal to the soak assembly, and a public xunit test method cannot take an
    // internal parameter type (CS0051).
    [Theory]
    [InlineData("DEBUG", "Debug")]
    [InlineData("info", "Info")]
    [InlineData("WARNING", "Warning")]
    [InlineData("warn", "Warning")]
    [InlineData("ERROR", "Error")]
    [InlineData("FATAL", "Fatal")]
    // A log level is never worth failing a two-week run over, so an unknown one falls back.
    [InlineData("nonsense", "Info")]
    public void LogLevelParsing(string value, string expected)
    {
        Environment.SetEnvironmentVariable("SOAK_TESTID", "s");
        Environment.SetEnvironmentVariable("SOAK_TOPIC", "t");
        Environment.SetEnvironmentVariable("SOAK_LOG_LEVEL", value);

        Assert.Equal(expected, SoakOptions.FromEnvironment().LogLevel.ToString());
    }
}
