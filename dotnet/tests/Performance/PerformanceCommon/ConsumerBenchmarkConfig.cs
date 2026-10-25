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

namespace Confluent.Kafka.Performance;

/// <summary>
/// The consumer benchmark's settings, read from the environment variables of Python's <c>Config</c>: the run shape
/// the engine uses and the fetch-size, batch-size and use-defaults inputs each per-client exe builds its consumer
/// from.
/// </summary>
public sealed class ConsumerBenchmarkConfig
{
    private const long NoDataTimeoutSeconds = 120;

    private static readonly string[] s_compressionTypes = { "none", "gzip", "snappy", "lz4", "zstd" };

    private ConsumerBenchmarkConfig()
    {
        BootstrapServers = "localhost:9092";
        Topic = "test-topic";
        ClientVersion = "3";
        GroupId = string.Empty;
    }

    public static long NoDataBudgetSeconds => NoDataTimeoutSeconds;

    public string BootstrapServers { get; private set; }

    public string Topic { get; private set; }

    public string ClientVersion { get; private set; }

    public string GroupId { get; private set; }

    public int WarmupSeconds { get; private set; } = 120;

    public int TestDurationSeconds { get; private set; } = 600;

    public int IntervalSeconds { get; private set; } = 1;

    public int PollTimeoutMs { get; private set; } = 1000;

    public int MessageSize { get; private set; } = 2048;

    public int Throughput { get; private set; } = 125000;

    public long NumMessages { get; private set; }

    public int Partitions { get; private set; } = -1;

    public int P99LimitMs { get; private set; }

    public int JoinTimeoutSeconds { get; private set; } = 120;

    public int SettleTimeoutSeconds { get; private set; } = 15;

    public string? KafkaBin { get; private set; }

    public string? CompressionType { get; private set; }

    public int FetchMinBytes { get; private set; } = 4 * 1024 * 1024;

    public int MaxPartitionFetchBytes { get; private set; } = 4 * 1024 * 1024;

    public int BatchSize { get; private set; } = 2000;

    public bool AsyncMode { get; private set; }

    public bool PollSingle { get; private set; }

    public bool UseDefaults { get; private set; }

    public bool CreateTopic { get; private set; }

    public int ReadinessMinRecords { get; private set; } = 20;

    public int ReadinessTimeoutSeconds { get; private set; } = 20;

    public static ConsumerBenchmarkConfig FromEnv()
    {
        var config = new ConsumerBenchmarkConfig
        {
            BootstrapServers = PerfEnv.GetString("BOOTSTRAP_SERVERS", "localhost:9092"),
            Topic = PerfEnv.GetString("TOPIC_NAME", "test-topic"),
            ClientVersion = PerfEnv.GetString("CLIENT_VERSION", "3"),
            WarmupSeconds = PerfEnv.GetInt("WARMUP_SECONDS", 120),
            TestDurationSeconds = PerfEnv.GetInt("TEST_DURATION_SECONDS", 600),
            IntervalSeconds = PerfEnv.GetInt("INTERVAL_SECONDS", 1),
            PollTimeoutMs = PerfEnv.GetInt("POLL_TIMEOUT_MS", 1000),
            MessageSize = PerfEnv.GetInt("VALUE_SIZE", 2048),
            Throughput = PerfEnv.GetInt("THROUGHPUT", 125000),
            NumMessages = PerfEnv.GetLong("NUM_MESSAGES", 0),
            Partitions = PerfEnv.GetInt("PARTITIONS", -1),
            P99LimitMs = PerfEnv.GetInt("P99_LIMIT_MS", 0),
            JoinTimeoutSeconds = PerfEnv.GetInt("JOIN_TIMEOUT_SECONDS", 120),
            SettleTimeoutSeconds = PerfEnv.GetInt("SETTLE_TIMEOUT_SECONDS", 15),
            KafkaBin = PerfEnv.GetStringOrNull("KAFKA_BIN"),
            CompressionType = CompressionTypeFromEnv(),
            FetchMinBytes = PerfEnv.GetInt("FETCH_MIN_BYTES", 4 * 1024 * 1024),
            MaxPartitionFetchBytes = PerfEnv.GetInt("MAX_PARTITION_FETCH_BYTES", 4 * 1024 * 1024),
            BatchSize = PerfEnv.GetInt("CONSUMER_BATCH_SIZE", 2000),
            AsyncMode = PerfEnv.GetBool("ASYNC", false),
            PollSingle = PerfEnv.GetBool("POLL_SINGLE", false),
            UseDefaults = PerfEnv.GetBool("USE_DEFAULTS", false),
            CreateTopic = PerfEnv.GetBool("CREATE_TOPIC", true),
            ReadinessMinRecords = PerfEnv.GetInt("READINESS_MIN_RECORDS", 20),
            ReadinessTimeoutSeconds = PerfEnv.GetInt("READINESS_TIMEOUT_SECONDS", 20),
        };

        config.GroupId = PerfEnv.GetString(
            "GROUP_ID",
            $"benchmark-{config.ClientVersion}-{DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}");
        return config;
    }

    private static string? CompressionTypeFromEnv()
    {
        string? compressionType = PerfEnv.GetStringOrNull("COMPRESSION_TYPE");
        if (string.IsNullOrEmpty(compressionType))
        {
            return null;
        }

        if (Array.IndexOf(s_compressionTypes, compressionType) < 0)
        {
            throw new ArgumentException(
                $"COMPRESSION_TYPE must be one of none, gzip, snappy, lz4, zstd; got '{compressionType}'");
        }

        return compressionType;
    }
}
