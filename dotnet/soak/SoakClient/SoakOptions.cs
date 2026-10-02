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
/// The soak's run configuration, read from <c>SOAK_*</c> environment variables.
/// <para>
/// ⚠ RECORDED DEVIATION from the Python original, which takes <c>argparse</c> flags
/// (<c>-i/-t/-r/-f/...</c>). The .NET convention in this repo is env-driven
/// (<c>PerfV2</c> / <c>PerfV3</c>), so <c>run.sh</c> exports <c>SOAK_*</c> instead of
/// building a flag array, and no CLI-parsing library is taken as a dependency. The
/// variable names match the ones <c>run.sh</c> already computed for the Python soak, so
/// an operator's muscle memory carries over; <c>SOAK_EXTRA_ARGS</c> becomes additional
/// env assignments.
/// </para>
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

    /// <summary>Test id; tags every metric and is used as the clients' <c>client.id</c>.</summary>
    internal string TestId { get; }

    /// <summary>The topic to produce to and consume from. One topic per soak instance.</summary>
    internal string Topic { get; }

    /// <summary>Variant label, emitted as the <c>variant</c> metric tag.</summary>
    internal string Variant { get; }

    /// <summary>Messages produced per second.</summary>
    internal double Rate { get; }

    /// <summary>Path to the <c>key=value</c> client configuration file, or null for none.</summary>
    internal string? ConfigFile { get; }

    /// <summary>JSONL metrics output path (appended, never truncated).</summary>
    internal string MetricsFile { get; }

    /// <summary>Overrides <c>bootstrap.servers</c> from the configuration file when set.</summary>
    internal string? Brokers { get; }

    /// <summary>Target serialized record size in bytes.</summary>
    internal int PayloadSize { get; }

    /// <summary>Partitions to create the topic with.</summary>
    internal int Partitions { get; }

    /// <summary>Replication factor for topic creation; <c>-1</c> means the broker default.</summary>
    internal int ReplicationFactor { get; }

    /// <summary>
    /// Delete and re-create the topic at startup. Destructive: never use with
    /// <c>run.sh</c>, which restarts the client in a loop.
    /// </summary>
    internal bool RecreateTopic { get; }

    /// <summary>Seconds between metrics windows.</summary>
    internal double MetricsIntervalSeconds { get; }

    /// <summary>Seconds between confirming offset commits.</summary>
    internal double CommitIntervalSeconds { get; }

    /// <summary>Consumer poll timeout, in seconds.</summary>
    internal double PollTimeoutSeconds { get; }

    /// <summary>
    /// Seconds without records before reporting a stall, and from which
    /// <c>consumer.recovery_ms</c> is measured.
    /// </summary>
    internal double StallThresholdSeconds { get; }

    /// <summary>Attempts before abandoning a record whose send throws a retriable error.</summary>
    internal int MaxSendAttempts { get; }

    /// <summary>
    /// Consecutive consumer poll failures before the run is aborted so the supervisor
    /// restarts it. Non-retriable errors abort after
    /// <see cref="SoakClient.NonRetriablePollFailureLimit"/> instead.
    /// </summary>
    internal int MaxPollFailures { get; }

    /// <summary>Exit after this many seconds; <c>0</c> runs forever.</summary>
    internal double RuntimeSeconds { get; }

    /// <summary>Hard-exit if shutdown takes longer than this, in seconds.</summary>
    internal double ShutdownTimeoutSeconds { get; }

    /// <summary>Minimum log severity emitted.</summary>
    internal SoakLogLevel LogLevel { get; }

    /// <summary>
    /// Reads the options from the environment, applying the same defaults the Python
    /// soak's <c>argparse</c> declares.
    /// </summary>
    /// <exception cref="ArgumentException">A required variable is missing, or a value is out of range.</exception>
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
            // Was 10, applied as a per-variant override for the rolled cluster; a 5 s
            // stall is worth flagging anywhere, so it is the uniform default.
            SoakEnv.GetDouble("SOAK_STALL_THRESHOLD", 5.0),
            SoakEnv.GetInt("SOAK_MAX_SEND_ATTEMPTS", 10),
            SoakEnv.GetInt("SOAK_MAX_POLL_FAILURES", 20),
            SoakEnv.GetDouble("SOAK_RUNTIME_SECONDS", 0.0),
            SoakEnv.GetDouble("SOAK_SHUTDOWN_TIMEOUT", 60.0),
            SoakLogger.ParseLevel(SoakEnv.GetStringOrNull("SOAK_LOG_LEVEL")));
    }
}
