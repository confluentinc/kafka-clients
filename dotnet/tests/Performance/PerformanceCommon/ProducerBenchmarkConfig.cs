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

    /// <summary>
    /// Pacing slice of the rate limiter in ms (<c>LIMIT_RPS_SLICE_MS</c>, default 1000; only read when
    /// <c>LIMIT_RPS</c> is set). The limiter checks the clock once per slice: it sends
    /// <see cref="LimitRpsSliceMessages"/> records at full speed, then sleeps until the slice's scheduled
    /// end. The default reproduces Python's per-second quota exactly (<c>producer_performance_test.py</c>
    /// checks every <c>limit_rps</c> messages). That quota is not a smooth offered load: the sender runs at
    /// full speed until the second's quota is sent and then idles, so the closer <c>LIMIT_RPS</c> is to the
    /// client's full-speed rate, the more of each second is an unthrottled burst. A slice of 5–10 ms spreads
    /// the same rate evenly, so the client can be measured below saturation.
    /// </summary>
    public int LimitRpsSliceMs { get; private set; } = 1000;

    /// <summary>
    /// Records per pacing slice: <c>LIMIT_RPS × LIMIT_RPS_SLICE_MS / 1000</c>, at least 1; 0 when no rate
    /// limit is set. At the default slice this is exactly <c>LIMIT_RPS</c>.
    /// </summary>
    public long LimitRpsSliceMessages =>
        LimitRps is int rps && rps > 0 ? Math.Max(1L, (long)rps * LimitRpsSliceMs / 1000) : 0;

    /// <summary>p99 latency budget in ms (<c>P99_LIMIT_MS</c>, default 0 = disabled); exceeding it fails the run.</summary>
    public int P99LimitMs { get; private set; }

    /// <summary>Whether to validate each returned <see cref="PerfRecordMetadata"/> (<c>DO_VERIFY</c>, default true).</summary>
    public bool DoVerify { get; private set; }

    /// <summary>Whether to drive the async (pipelined) path instead of the sync (serial-blocking) path (<c>ASYNC</c>, default false).</summary>
    public bool Async { get; private set; }

    /// <summary>
    /// Whether the async send loop waits for each record to be <b>accepted</b> by the client before it sends
    /// the next one (<c>AWAIT_ACCEPTED</c>, default <b>true</b>; used only on the async path). A send has two
    /// stages: accepted (Java's <c>send()</c> returning) and delivered. With <c>False</c> the loop hands both
    /// stages to the recorder and sends the next record at once; a send whose acceptance fails is counted by
    /// the recorder like one whose call throws. This binding's async <c>Send</c> has a first stage that can
    /// stay pending once its bound on records not yet handed to the send-batch thread is reached, so with
    /// <c>True</c> that bound throttles this loop. With <c>False</c> it no longer does: records accumulate
    /// inside the client, and its memory grows if they are offered faster than the cluster drains them. For
    /// PerfV2 (confluent-kafka-dotnet), whose first stage is always complete, <c>AWAIT_ACCEPTED</c> has no
    /// effect. Parsed strictly (<c>True</c> or <c>False</c>): unlike the other flags it defaults to true, so a
    /// misspelt value must not silently select the other mode.
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
        };

        string? awaitAccepted = PerfEnv.GetStringOrNull("AWAIT_ACCEPTED");
        config.AwaitAccepted = awaitAccepted switch
        {
            null or "" or "True" => true,
            "False" => false,
            _ => throw new ArgumentException($"AWAIT_ACCEPTED must be True or False, not '{awaitAccepted}'"),
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

            config.LimitRpsSliceMs = PerfEnv.GetInt("LIMIT_RPS_SLICE_MS", 1000);
            if (config.LimitRpsSliceMs <= 0)
            {
                throw new ArgumentException("LIMIT_RPS_SLICE_MS must be positive");
            }

            // Run for the specified duration at the target rate (Python: num_messages = limit_rps * duration).
            // (long) on the FIRST operand, so the multiplication itself is 64-bit — casting the result
            // would truncate before the widening (Python: num_messages = int(limit_rps * test_duration_s)).
            config.NumMessages = (long)config.LimitRps.Value * config.TestDurationSeconds;
        }

        return config;
    }
}
