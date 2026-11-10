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
/// the engine uses and the fetch and poll-size inputs each per-client exe builds its consumer from. A fetch or
/// poll-size setting that is <c>null</c> keeps the client's default.
/// </summary>
public sealed class ConsumerBenchmarkConfig
{
    private const long NoDataTimeoutSeconds = 120;

    // The fetch size the benchmark tunes unless USE_DEFAULTS=True.
    private const int TunedFetchBytes = 4 * 1024 * 1024;

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

    public int? FetchMinBytes { get; private set; } = TunedFetchBytes;

    public int? MaxPartitionFetchBytes { get; private set; } = TunedFetchBytes;

    public int? FetchMaxWaitMs { get; private set; }

    /// <summary>The most records one poll returns, <c>MAX_POLL_RECORDS</c>; <c>null</c> when it is unset.</summary>
    public int? MaxPollRecords { get; private set; }

    /// <summary>
    /// <c>CONSUMER_BATCH_SIZE</c> (default 2500, as Python's): both harnesses' batch when neither <c>MAX_POLL_RECORDS</c>
    /// nor <c>USE_DEFAULTS=True</c> sets it, so a run outside the driver keeps its tuned batch.
    /// </summary>
    public int BatchSize { get; private set; } = 2500;

    public bool AsyncMode { get; private set; }

    public bool PollSingle { get; private set; }

    public bool UseDefaults { get; private set; }

    public bool CreateTopic { get; private set; }

    public int ReadinessMinRecords { get; private set; } = 20;

    public int ReadinessTimeoutSeconds { get; private set; } = 20;

    public string ResultsFile { get; private set; } = "results.json";

    public static ConsumerBenchmarkConfig FromEnv()
    {
        bool useDefaults = PerfEnv.GetBool("USE_DEFAULTS", false);
        int? tunedFetch = useDefaults ? null : TunedFetchBytes;
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
            FetchMinBytes = OptionalInt("FETCH_MIN_BYTES", tunedFetch),
            MaxPartitionFetchBytes = OptionalInt("MAX_PARTITION_FETCH_BYTES", tunedFetch),
            FetchMaxWaitMs = OptionalInt("FETCH_MAX_WAIT_MS", null),
            MaxPollRecords = OptionalInt("MAX_POLL_RECORDS", null),
            BatchSize = PerfEnv.GetInt("CONSUMER_BATCH_SIZE", 2500),
            AsyncMode = PerfEnv.GetBool("ASYNC", false),
            PollSingle = PerfEnv.GetBool("POLL_SINGLE", false),
            UseDefaults = useDefaults,
            CreateTopic = PerfEnv.GetBool("CREATE_TOPIC", true),
            ReadinessMinRecords = PerfEnv.GetInt("READINESS_MIN_RECORDS", 20),
            ReadinessTimeoutSeconds = PerfEnv.GetInt("READINESS_TIMEOUT_SECONDS", 20),
            ResultsFile = ResultsFileFromEnv(),
        };

        config.GroupId = PerfEnv.GetString(
            "GROUP_ID",
            $"benchmark-{config.ClientVersion}-{DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}");
        return config;
    }

    // Unset or empty keeps the fallback, as Python's _env_int.
    private static int? OptionalInt(string name, int? fallback)
    {
        string? value = PerfEnv.GetStringOrNull(name);
        return string.IsNullOrEmpty(value) ? fallback : int.Parse(value, CultureInfo.InvariantCulture);
    }

    // An empty RESULTS_FILE is the default, as Python's `os.getenv("RESULTS_FILE") or "results.json"`.
    private static string ResultsFileFromEnv()
    {
        string? path = PerfEnv.GetStringOrNull("RESULTS_FILE");
        return string.IsNullOrEmpty(path) ? "results.json" : path;
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
