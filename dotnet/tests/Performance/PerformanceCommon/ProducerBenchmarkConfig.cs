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

    /// <summary>
    /// Optional target rate (<c>LIMIT_RPS</c>); when set, <see cref="NumMessages"/> = rate × duration. Unset,
    /// empty, <c>0</c> or negative all mean max rate (<see langword="null"/>), as in Python.
    /// </summary>
    public int? LimitRps { get; private set; }

    /// <summary>
    /// Records between two clock checks of the rate limiter: <c>max(floor(LIMIT_RPS / 10), 1)</c>, about
    /// 100 ms of traffic; 0 when there is no rate limit. At each checkpoint the sender sleeps until
    /// <c>first send + records sent / LIMIT_RPS</c> (Python <c>pacing_wait_s</c>, and the C, Rust and Java
    /// harnesses). The deadline comes from the cumulative count, so a rate that is not a multiple of 10 is
    /// honoured exactly and an oversleep is made up over the following checkpoints.
    /// </summary>
    public long PacingCheckpoint => LimitRps is int rps ? Math.Max(rps / 10, 1) : 0;

    /// <summary>
    /// Where the machine-readable run summary is written (<c>RESULTS_FILE</c>, default <c>results.json</c>,
    /// relative to the working directory). The same keys as every other producer harness's summary.
    /// </summary>
    public string ResultsFile { get; private set; } = "results.json";

    /// <summary>p99 latency budget in ms (<c>P99_LIMIT_MS</c>, default 0 = disabled); exceeding it fails the run.</summary>
    public int P99LimitMs { get; private set; }

    /// <summary>Whether to validate each returned <see cref="PerfRecordMetadata"/> (<c>DO_VERIFY</c>, default true).</summary>
    public bool DoVerify { get; private set; }

    /// <summary>Whether to drive the async path (Python <c>async_main</c>) instead of the sync path (Python <c>main</c>) (<c>ASYNC</c>, default false).</summary>
    public bool Async { get; private set; }

    /// <summary>
    /// Whether the async send loop waits for each record to be <b>accepted</b> by the client before it sends
    /// the next one (<c>AWAIT_ACCEPTED</c>, default <b>true</b>; used only on the async path). A send has two
    /// stages: accepted (Java's <c>send()</c> returning) and delivered. With <c>False</c> the loop sends the
    /// next record at once; a send whose acceptance later fails is counted as a failed delivery, since it was
    /// already counted as sent. This binding's async <c>Send</c> has a first stage that can
    /// stay pending once its bound on records not yet handed to the send-batch thread is reached, so with
    /// <c>True</c> that bound throttles this loop. With <c>False</c> it no longer does: records accumulate
    /// inside the client, and its memory grows if they are offered faster than the cluster drains them. For
    /// PerfV2 (confluent-kafka-dotnet), whose first stage is always complete, <c>AWAIT_ACCEPTED</c> has no
    /// effect. Parsed strictly (<c>True</c> or <c>False</c>; unset or empty means true, anything else throws),
    /// so a misspelt value cannot silently select the non-default mode.
    /// </summary>
    public bool AwaitAccepted { get; private set; } = true;

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
            // A value <= 0 means max rate, the same as leaving LIMIT_RPS unset (Python sets limit_rps to None
            // for it, so the run neither paces nor derives a message count).
            config.LimitRps = rps;

            // Run for the specified duration at the target rate (Python: num_messages = limit_rps * duration).
            // (long) on the FIRST operand, so the multiplication itself is 64-bit — casting the result
            // would truncate before the widening (Python: num_messages = int(limit_rps * test_duration_s)).
            config.NumMessages = (long)rps * config.TestDurationSeconds;
        }

        return config;
    }
}
