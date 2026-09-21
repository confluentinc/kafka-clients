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
/// The producer benchmark's run-shape knobs, parsed from the environment — the subset of
/// <c>producer_performance_test.py</c>'s module globals the client-agnostic engine needs (topic, sizes,
/// counts, durations, rate limit, p99 budget, verify toggle, async toggle). The client-specific producer
/// config (batch.size, compression, SASL form, …) is built by the per-client exe, not here.
/// </summary>
public sealed class ProducerBenchmarkConfig
{
    /// <summary>Seconds the harness keeps sampling after the measured interval (cooldown); const 10 (Python <c>POST_TEST_AWAIT_SECONDS</c>).</summary>
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

    /// <summary>The destination topic (<c>TOPIC_NAME</c>, default <c>test-topic</c>).</summary>
    public string TopicName { get; private set; }

    /// <summary>
    /// Cross-language client selector (<c>CLIENT_VERSION</c>, default <c>3</c>); each exe's
    /// <c>Program.Main</c> pins it up front. <see cref="ProducerBenchmark"/>'s verify predicate reads it —
    /// Python's <c>verify_message</c> (v2/ckd) requires <c>timestamp &gt; 0</c> while
    /// <c>verify_record_metadata</c> (v3) requires <c>timestamp &gt;= 0</c>, and that asymmetry is
    /// preserved rather than smoothed over.
    /// </summary>
    public string ClientVersion { get; private set; }

    /// <summary>Key size in bytes (<c>KEY_SIZE</c>, default 0 = no key).</summary>
    public int KeySize { get; private set; }

    /// <summary>Value size in bytes (<c>VALUE_SIZE</c>, default 2048).</summary>
    public int ValueSize { get; private set; }

    /// <summary>Per-message wire size used for the byte metrics and the async queue bound: key + value.</summary>
    public int MessageSize => KeySize + ValueSize;

    /// <summary>
    /// Total messages to send (<c>NUM_MESSAGES</c>, default 0 = duration-bounded); overridden by
    /// <c>LIMIT_RPS</c>. 64-bit because it is <c>LIMIT_RPS × TEST_DURATION_SECONDS</c>: at 32 bits an
    /// extreme-but-legal setting (5,000,000 msg/s over 600 s = 3e9) overflowed to a NEGATIVE value, which
    /// <see cref="ProducerBenchmark"/> reads as "no cap" — so the benchmark silently ignored the message
    /// count it was asked for. Python does the same arithmetic in arbitrary precision and just gets it
    /// right.
    /// </summary>
    public long NumMessages { get; private set; }

    /// <summary>Measured duration in seconds (<c>TEST_DURATION_SECONDS</c>, default 600).</summary>
    public int TestDurationSeconds { get; private set; }

    /// <summary>Warmup duration in seconds (<c>WARMUP_SECONDS</c>, default 120); warmup sends are never recorded.</summary>
    public int WarmupSeconds { get; private set; }

    /// <summary>Optional target rate (<c>LIMIT_RPS</c>); when set, <see cref="NumMessages"/> = rate × duration.</summary>
    public int? LimitRps { get; private set; }

    /// <summary>p99 latency budget in ms (<c>P99_LIMIT_MS</c>, default 0 = disabled); exceeding it fails the run.</summary>
    public int P99LimitMs { get; private set; }

    /// <summary>Whether to validate each returned <see cref="PerfRecordMetadata"/> (<c>DO_VERIFY</c>, default true).</summary>
    public bool DoVerify { get; private set; }

    /// <summary>Whether to drive the async (pipelined) path instead of the sync (serial-blocking) path (<c>ASYNC</c>, default false).</summary>
    public bool Async { get; private set; }

    /// <summary>
    /// Whether the harness should (re)create the topic before producing (<c>CREATE_TOPIC</c>, default
    /// <b>true</b>, matching Python). Provisioning is per-exe (<c>PerfV3</c>/<c>PerfV2</c>'s own
    /// <c>TopicProvisioning.RecreateTopic</c>) rather than in this client-agnostic config type, since an
    /// AdminClient is unavoidably client-specific and <c>PerformanceCommon</c> carries no client
    /// dependency (M13/P1 D8).
    /// </summary>
    public bool CreateTopic { get; private set; }

    /// <summary>Partition count for topic (re)creation (<c>PARTITIONS</c>, default -1 = broker default).</summary>
    public int Partitions { get; private set; } = -1;

    /// <summary>Parses the producer benchmark run-shape config from the environment (§5.2).</summary>
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
        };

        string? limitRps = PerfEnv.GetStringOrNull("LIMIT_RPS");
        if (!string.IsNullOrEmpty(limitRps))
        {
            config.LimitRps = PerfEnv.GetInt("LIMIT_RPS", 0);
            if (config.LimitRps.Value <= 0)
            {
                // Python's message_generator hard-fails on limit_rps <= 0 ("limit_rps must be positive")
                // rather than treating 0/negative as "unbounded" — that meaning belongs to LIMIT_RPS being
                // unset entirely. Match that instead of silently falling through to max-rate, which would
                // run a materially different benchmark than what was asked for.
                throw new ArgumentException("LIMIT_RPS must be positive");
            }

            // Run for the specified duration at the target rate (Python: num_messages = limit_rps * duration).
            // (long) on the FIRST operand, so the multiplication itself is 64-bit — casting the result
            // would truncate before the widening (Python: num_messages = int(limit_rps * test_duration_s)).
            config.NumMessages = (long)config.LimitRps.Value * config.TestDurationSeconds;
        }

        return config;
    }
}
