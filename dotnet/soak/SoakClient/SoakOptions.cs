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
using System.Globalization;

namespace Confluent.Kafka.Soak;

/// <summary>
/// The soak's run configuration, from <c>SOAK_*</c> environment variables: a deviation from
/// Python's <c>argparse</c> flags, as the repo's .NET tools are env-driven; the names are the ones
/// <c>run.sh</c> computes for the Python soak.
/// </summary>
internal sealed class SoakOptions
{
    private SoakOptions(
        string testId,
        string topic,
        string variant,
        double rate,
        string? configFile,
        string metricsFile,
        string? brokers,
        int payloadSize,
        int partitions,
        int replicationFactor,
        bool recreateTopic,
        double metricsIntervalSeconds,
        double commitIntervalSeconds,
        double pollTimeoutSeconds,
        double stallThresholdSeconds,
        int maxSendAttempts,
        int maxPollFailures,
        double runtimeSeconds,
        double shutdownTimeoutSeconds,
        SoakLogLevel logLevel)
    {
        TestId = testId;
        Topic = topic;
        Variant = variant;
        Rate = rate;
        ConfigFile = configFile;
        MetricsFile = metricsFile;
        Brokers = brokers;
        PayloadSize = payloadSize;
        Partitions = partitions;
        ReplicationFactor = replicationFactor;
        RecreateTopic = recreateTopic;
        MetricsIntervalSeconds = metricsIntervalSeconds;
        CommitIntervalSeconds = commitIntervalSeconds;
        PollTimeoutSeconds = pollTimeoutSeconds;
        StallThresholdSeconds = stallThresholdSeconds;
        MaxSendAttempts = maxSendAttempts;
        MaxPollFailures = maxPollFailures;
        RuntimeSeconds = runtimeSeconds;
        ShutdownTimeoutSeconds = shutdownTimeoutSeconds;
        LogLevel = logLevel;
    }

    internal string TestId { get; }

    internal string Topic { get; }

    internal string Variant { get; }

    internal double Rate { get; }

    internal string? ConfigFile { get; }

    internal string MetricsFile { get; }

    internal string? Brokers { get; }

    internal int PayloadSize { get; }

    internal int Partitions { get; }

    internal int ReplicationFactor { get; }

    // Destructive: never set under run.sh, which restarts the client in a loop.
    internal bool RecreateTopic { get; }

    internal double MetricsIntervalSeconds { get; }

    internal double CommitIntervalSeconds { get; }

    internal double PollTimeoutSeconds { get; }

    internal double StallThresholdSeconds { get; }

    internal int MaxSendAttempts { get; }

    internal int MaxPollFailures { get; }

    internal double RuntimeSeconds { get; }

    internal double ShutdownTimeoutSeconds { get; }

    internal SoakLogLevel LogLevel { get; }

    // Defaults as the Python soak's argparse.
    internal static SoakOptions FromEnvironment()
    {
        string testId = SoakEnv.GetString("SOAK_TESTID", string.Empty);
        if (testId.Length == 0)
        {
            throw new ArgumentException("SOAK_TESTID must be set (it tags every metric and names the clients)");
        }

        string topic = SoakEnv.GetString("SOAK_TOPIC", string.Empty);
        if (topic.Length == 0)
        {
            throw new ArgumentException("SOAK_TOPIC must be set (a unique topic per soak instance)");
        }

        double rate = SoakEnv.GetDouble("SOAK_RATE", 80.0);
        if (rate <= 0)
        {
            throw new ArgumentException(string.Format(
                CultureInfo.InvariantCulture,
                "SOAK_RATE must be greater than zero (got {0})",
                rate));
        }

        string variant = SoakEnv.GetString("SOAK_VARIANT", "unspecified");
        string metricsFile = SoakEnv.GetString(
            "SOAK_METRICS_FILE",
            string.Format(CultureInfo.InvariantCulture, "soak-metrics-{0}-{1}.jsonl", variant, testId));

        return new SoakOptions(
            testId,
            topic,
            variant,
            rate,
            SoakEnv.GetStringOrNull("SOAK_CONFIG_FILE"),
            metricsFile,
            SoakEnv.GetStringOrNull("SOAK_BROKERS"),
            SoakEnv.GetInt("SOAK_PAYLOAD_SIZE", 50),
            SoakEnv.GetInt("SOAK_PARTITIONS", 2),
            SoakEnv.GetInt("SOAK_REPLICATION_FACTOR", -1),
            SoakEnv.GetBool("SOAK_RECREATE_TOPIC", false),
            SoakEnv.GetDouble("SOAK_METRICS_INTERVAL", 10.0),
            SoakEnv.GetDouble("SOAK_COMMIT_INTERVAL", 5.0),
            SoakEnv.GetDouble("SOAK_POLL_TIMEOUT", 1.0),
            SoakEnv.GetDouble("SOAK_STALL_THRESHOLD", 5.0),
            SoakEnv.GetInt("SOAK_MAX_SEND_ATTEMPTS", 10),
            SoakEnv.GetInt("SOAK_MAX_POLL_FAILURES", 20),
            SoakEnv.GetDouble("SOAK_RUNTIME_SECONDS", 0.0),
            SoakEnv.GetDouble("SOAK_SHUTDOWN_TIMEOUT", 60.0),
            SoakLogger.ParseLevel(SoakEnv.GetStringOrNull("SOAK_LOG_LEVEL")));
    }
}
