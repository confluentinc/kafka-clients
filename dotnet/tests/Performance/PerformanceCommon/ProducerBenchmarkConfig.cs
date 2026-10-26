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

namespace Confluent.Kafka.Performance;

/// <summary>
/// The producer benchmark's run shape, read from the environment variables of Python's
/// <c>producer_performance_test.py</c>; each per-client exe builds its own producer config.
/// </summary>
public sealed class ProducerBenchmarkConfig
{
    public const int PostTestAwaitSeconds = 10;

    private ProducerBenchmarkConfig()
    {
        TopicName = "test-topic";
        ValueSize = 2048;
        TestDurationSeconds = 600;
        WarmupSeconds = 120;
        DoVerify = true;
        ClientVersion = "3";
    }

    public string TopicName { get; private set; }

    public string ClientVersion { get; private set; }

    public int KeySize { get; private set; }

    public int ValueSize { get; private set; }

    public int MessageSize => KeySize + ValueSize;

    public long NumMessages { get; private set; }

    public int TestDurationSeconds { get; private set; }

    public int WarmupSeconds { get; private set; }

    public int? LimitRps { get; private set; }

    public long PacingCheckpoint => LimitRps is int rps ? Math.Max(rps / 10, 1) : 0;

    public string ResultsFile { get; private set; } = "results.json";

    public int P99LimitMs { get; private set; }

    public bool DoVerify { get; private set; }

    public bool Async { get; private set; }

    public bool AwaitAccepted { get; private set; } = true;

    public bool CreateTopic { get; private set; }

    public int Partitions { get; private set; } = -1;

    public static ProducerBenchmarkConfig FromEnv()
    {
        var config = new ProducerBenchmarkConfig
        {
            TopicName = PerfEnv.GetString("TOPIC_NAME", "test-topic"),
            KeySize = PerfEnv.GetInt("KEY_SIZE", 0),
            ValueSize = PerfEnv.GetInt("VALUE_SIZE", 2048),
            NumMessages = PerfEnv.GetLong("NUM_MESSAGES", 0),
            TestDurationSeconds = PerfEnv.GetInt("TEST_DURATION_SECONDS", 600),
            WarmupSeconds = PerfEnv.GetInt("WARMUP_SECONDS", 120),
            P99LimitMs = PerfEnv.GetInt("P99_LIMIT_MS", 0),
            DoVerify = PerfEnv.GetBool("DO_VERIFY", true),
            Async = PerfEnv.GetBool("ASYNC", false),
            CreateTopic = PerfEnv.GetBool("CREATE_TOPIC", true),
            Partitions = PerfEnv.GetInt("PARTITIONS", -1),
            ClientVersion = PerfEnv.GetString("CLIENT_VERSION", "3"),
            ResultsFile = PerfEnv.GetString("RESULTS_FILE", "results.json"),
        };

        string? awaitAccepted = PerfEnv.GetStringOrNull("AWAIT_ACCEPTED");
        config.AwaitAccepted = awaitAccepted switch
        {
            null or "" or "True" => true,
            "False" => false,
            _ => throw new ArgumentException($"AWAIT_ACCEPTED must be True or False, not '{awaitAccepted}'"),
        };

        string? limitRps = PerfEnv.GetStringOrNull("LIMIT_RPS");
        if (!string.IsNullOrEmpty(limitRps) && PerfEnv.GetInt("LIMIT_RPS", 0) is int rps && rps > 0)
        {
            config.LimitRps = rps;

            // Widened before the multiplication, which would otherwise overflow 32 bits.
            config.NumMessages = (long)rps * config.TestDurationSeconds;
        }

        return config;
    }
}
