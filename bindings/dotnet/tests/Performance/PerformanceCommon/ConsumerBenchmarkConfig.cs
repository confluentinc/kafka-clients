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
/// The consumer benchmark configuration parsed from the environment — the C# analog of
/// <c>consumer_performance_test.py</c>'s <c>Config</c> (§6.2). Holds both the run-shape knobs the engine
/// uses and the client-config inputs (fetch sizes, batch size, use-defaults) the per-client exe reads to
/// build its consumer.
/// </summary>
public sealed class ConsumerBenchmarkConfig
{
    private const long NoDataTimeoutSeconds = 120;

    private ConsumerBenchmarkConfig()
    {
        BootstrapServers = "localhost:9092";
        Topic = "test-topic";
        ClientVersion = "3";
        GroupId = string.Empty;
    }

    /// <summary>Wall time (s) a benchmark waits for any record before aborting the run (Python's fixed 120 s no-data budget).</summary>
    public static long NoDataBudgetSeconds => NoDataTimeoutSeconds;

    /// <summary>Bootstrap servers (<c>BOOTSTRAP_SERVERS</c>, default <c>localhost:9092</c>).</summary>
    public string BootstrapServers { get; private set; }

    /// <summary>Topic to consume (<c>TOPIC_NAME</c>, default <c>test-topic</c>).</summary>
    public string Topic { get; private set; }

    /// <summary>Cross-language client selector, recorded in <c>results.json</c> (<c>CLIENT_VERSION</c>, default <c>3</c>).</summary>
    public string ClientVersion { get; private set; }

    /// <summary>Consumer group id (<c>GROUP_ID</c>, default <c>benchmark-&lt;ver&gt;-&lt;epoch&gt;</c>).</summary>
    public string GroupId { get; private set; }

    /// <summary>Warmup duration in seconds (<c>WARMUP_SECONDS</c>, default 120), excluded from the stats.</summary>
    public int WarmupSeconds { get; private set; } = 120;

    /// <summary>Measured duration in seconds (<c>TEST_DURATION_SECONDS</c>, default 600).</summary>
    public int TestDurationSeconds { get; private set; } = 600;

    /// <summary>Sampler tick in seconds (<c>INTERVAL_SECONDS</c>, default 1).</summary>
    public int IntervalSeconds { get; private set; } = 1;

    /// <summary>Poll timeout in ms (<c>POLL_TIMEOUT_MS</c>, default 1000).</summary>
    public int PollTimeoutMs { get; private set; } = 1000;

    /// <summary>Value size in bytes (<c>VALUE_SIZE</c>, default 2048), used for the throughput MiB/s figure.</summary>
    public int MessageSize { get; private set; } = 2048;

    /// <summary>Producer target throughput for the <c>KAFKA_BIN</c> load driver (<c>THROUGHPUT</c>, default 125000).</summary>
    public int Throughput { get; private set; } = 125000;

    /// <summary>Measured-message cap (<c>NUM_MESSAGES</c>, default 0 = duration-bounded).</summary>
    public int NumMessages { get; private set; }

    /// <summary>Partition count for topic creation (<c>PARTITIONS</c>, default -1 = broker default); provisioning is external.</summary>
    public int Partitions { get; private set; } = -1;

    /// <summary>p99 latency budget in ms (<c>P99_LIMIT_MS</c>, default 0 = disabled).</summary>
    public int P99LimitMs { get; private set; }

    /// <summary>Assignment-wait abort in seconds (<c>JOIN_TIMEOUT_SECONDS</c>, default 120).</summary>
    public int JoinTimeoutSeconds { get; private set; } = 120;

    /// <summary>Live-edge settle abort in seconds (<c>SETTLE_TIMEOUT_SECONDS</c>, default 15).</summary>
    public int SettleTimeoutSeconds { get; private set; } = 15;

    /// <summary>In-container load-driver directory (<c>KAFKA_BIN</c>); when set, the engine self-spawns a producer.</summary>
    public string? KafkaBin { get; private set; }

    /// <summary><c>fetch.min.bytes</c> (<c>FETCH_MIN_BYTES</c>, default 4 MiB).</summary>
    public int FetchMinBytes { get; private set; } = 4 * 1024 * 1024;

    /// <summary><c>max.partition.fetch.bytes</c> (<c>MAX_PARTITION_FETCH_BYTES</c>, default 4 MiB).</summary>
    public int MaxPartitionFetchBytes { get; private set; } = 4 * 1024 * 1024;

    /// <summary>Batch size per poll (<c>CONSUMER_BATCH_SIZE</c>, default 2000); drives <c>max.poll.records = batch + 500</c>.</summary>
    public int BatchSize { get; private set; } = 2000;

    /// <summary>Whether to drive the async consumer (<c>ASYNC</c>, default false).</summary>
    public bool AsyncMode { get; private set; }

    /// <summary>Whether to use the single-message poll path (<c>POLL_SINGLE</c>, default false); delegates to batch poll.</summary>
    public bool PollSingle { get; private set; }

    /// <summary>Whether to skip the explicit consumer config keys (<c>USE_DEFAULTS</c>, default false).</summary>
    public bool UseDefaults { get; private set; }

    /// <summary>Whether the harness should (re)create the topic (<c>CREATE_TOPIC</c>); default <b>false</b> for .NET (no AdminClient, D6).</summary>
    public bool CreateTopic { get; private set; }

    /// <summary>Parses the consumer benchmark config from the environment (§6.2).</summary>
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
            NumMessages = PerfEnv.GetInt("NUM_MESSAGES", 0),
            Partitions = PerfEnv.GetInt("PARTITIONS", -1),
            P99LimitMs = PerfEnv.GetInt("P99_LIMIT_MS", 0),
            JoinTimeoutSeconds = PerfEnv.GetInt("JOIN_TIMEOUT_SECONDS", 120),
            SettleTimeoutSeconds = PerfEnv.GetInt("SETTLE_TIMEOUT_SECONDS", 15),
            KafkaBin = PerfEnv.GetStringOrNull("KAFKA_BIN"),
            FetchMinBytes = PerfEnv.GetInt("FETCH_MIN_BYTES", 4 * 1024 * 1024),
            MaxPartitionFetchBytes = PerfEnv.GetInt("MAX_PARTITION_FETCH_BYTES", 4 * 1024 * 1024),
            BatchSize = PerfEnv.GetInt("CONSUMER_BATCH_SIZE", 2000),
            AsyncMode = PerfEnv.GetBool("ASYNC", false),
            PollSingle = PerfEnv.GetBool("POLL_SINGLE", false),
            UseDefaults = PerfEnv.GetBool("USE_DEFAULTS", false),
            // D6: default False for .NET (no in-harness AdminClient); provisioning is external.
            CreateTopic = PerfEnv.GetBool("CREATE_TOPIC", false),
        };

        config.GroupId = PerfEnv.GetString(
            "GROUP_ID",
            $"benchmark-{config.ClientVersion}-{DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}");
        return config;
    }
}
