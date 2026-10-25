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
using System.Collections.Generic;
using System.Threading.Tasks;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// The perf smoke, as Python's producer and consumer <c>e2e_latency</c> tests, each run sync and async here: it runs
/// the built PerfV3 exe as a child process against <see cref="KafkaBrokerFixture"/>'s broker with the shared budget
/// (100 rps, 10 s, p99 ≤ 70 ms, 2048-byte values) and asserts a clean exit.
/// </summary>
public sealed class PerfV3SmokeTests : IClassFixture<KafkaBrokerFixture>
{
    private const string SmokeClientVersion = "3";

    // By platform, not a drift probe, as Python's _SKIP_CROSS_CLOCK; the producer smokes use one clock and still run.
    private const string CrossClockReason =
        "consumer e2e latency needs the feeder and consumer to share a clock, which macOS does not " +
        "guarantee across the Docker VM boundary (Colima drifts; Docker Desktop currently does not). " +
        "Skipped unconditionally on macOS for parity with the Python suite; runs on Linux, where the " +
        "broker container shares the host clock";

    private static readonly bool s_skipCrossClock = OperatingSystem.IsMacOS();

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public PerfV3SmokeTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public Task Producer_Smoke_Sync() => RunProducerSmokeAsync("producer-perf-smoke", asyncMode: false);

    [SkippableFact]
    public Task Producer_Smoke_Async() => RunProducerSmokeAsync("producer-perf-smoke-async", asyncMode: true);

    [SkippableFact]
    public Task Consumer_Smoke_Sync() => RunConsumerSmokeAsync("consumer-perf-smoke", asyncMode: false);

    [SkippableFact]
    public Task Consumer_Smoke_Async() => RunConsumerSmokeAsync("consumer-perf-smoke-async", asyncMode: true);

    private async Task RunProducerSmokeAsync(string topic, bool asyncMode)
    {
        await _broker.EnsureStartedAsync(_output).ConfigureAwait(false);
        await _broker.CreateTopicAsync(topic, partitions: 4).ConfigureAwait(false);

        var env = new Dictionary<string, string>
        {
            ["MODE"] = "producer",
            ["BOOTSTRAP_SERVERS"] = _broker.ExternalBootstrap!,
            ["TOPIC_NAME"] = topic,
            ["CLIENT_VERSION"] = SmokeClientVersion,
            ["ASYNC"] = asyncMode ? "True" : "False",
            ["WARMUP_SECONDS"] = "0",
            ["TEST_DURATION_SECONDS"] = "10",
            ["LIMIT_RPS"] = "100",
            ["VALUE_SIZE"] = "2048",
            // Only P99_LIMIT_MS passes through from the caller's environment, as in Python (0 disables the check).
            ["P99_LIMIT_MS"] = PerfEnv.GetString("P99_LIMIT_MS", "70"),
            ["DO_VERIFY"] = "False",
            ["CREATE_TOPIC"] = "False",
        };

        (int exitCode, string stdout, string stderr) = await PerfV3Process.RunAsync(_output, env, removeKeys: Array.Empty<string>()).ConfigureAwait(false);
        _output.WriteLine(stdout);
        _output.WriteLine(stderr);
        Assert.True(exitCode == 0, $"producer perf run failed (rc={exitCode}); see output above");
    }

    private async Task RunConsumerSmokeAsync(string topic, bool asyncMode)
    {
        // Before EnsureStartedAsync, so PERF_REQUIRE_DOCKER cannot turn this platform skip into a failure.
        Skip.If(s_skipCrossClock, CrossClockReason);

        await _broker.EnsureStartedAsync(_output).ConfigureAwait(false);
        await _broker.CreateTopicAsync(topic, partitions: 4).ConfigureAwait(false);

        // As Python's _smoke_env: FETCH_MIN_BYTES=1 so low-rate fetches return at once; the load is in-container.
        var env = new Dictionary<string, string>
        {
            ["MODE"] = "consumer",
            ["BOOTSTRAP_SERVERS"] = _broker.ExternalBootstrap!,
            ["TOPIC_NAME"] = topic,
            ["CLIENT_VERSION"] = SmokeClientVersion,
            ["ASYNC"] = asyncMode ? "True" : "False",
            ["WARMUP_SECONDS"] = "0",
            ["TEST_DURATION_SECONDS"] = "10",
            ["INTERVAL_SECONDS"] = "1",
            ["POLL_TIMEOUT_MS"] = "500",
            ["VALUE_SIZE"] = "2048",
            ["FETCH_MIN_BYTES"] = "1",
            ["P99_LIMIT_MS"] = PerfEnv.GetString("P99_LIMIT_MS", "70"),
            ["JOIN_TIMEOUT_SECONDS"] = "60",
            ["SETTLE_TIMEOUT_SECONDS"] = "5",
            ["CREATE_TOPIC"] = "False",
        };

        // As Python's _run_consumer_smoke: a SETUP_NOT_READY exit (a slow feeder) restarts the feeder and retries.
        int attempts = PerfEnv.GetInt("PERF_SETUP_ATTEMPTS", 3);

        Assert.True(
            attempts >= 1,
            $"PERF_SETUP_ATTEMPTS must be at least 1 (got {attempts}); a lower value would run no attempt " +
            "at all and report a pass having measured nothing.");

        int exitCode = -1;
        int attempt = 0;
        for (attempt = 1; attempt <= attempts; attempt++)
        {
            await _broker.ProducePerfInContainerAsync(topic, numRecords: 10000, recordSize: 2048, throughput: 100).ConfigureAwait(false);

            string stdout;
            string stderr;
            try
            {
                (exitCode, stdout, stderr) = await PerfV3Process.RunAsync(_output, env, removeKeys: new[] { "KAFKA_BIN" }).ConfigureAwait(false);
            }
            finally
            {
                // Unlike Python, stop this attempt's feeder, so a retry does not stack a second one.
                long killed = await _broker.StopPerfInContainerAsync(topic).ConfigureAwait(false);
                _output.WriteLine($">>> stopped in-container feeder for {topic} (pkill rc={killed})");
            }

            _output.WriteLine(stdout);
            _output.WriteLine(stderr);

            if (exitCode == ConsumerBenchmarkResult.SetupNotReadyExitCode && attempt < attempts)
            {
                _output.WriteLine($">>> pipeline not ready (attempt {attempt}/{attempts}); restarting feeder and retrying");
                continue;
            }

            break;
        }

        Assert.True(exitCode == 0, $"consumer perf run failed (rc={exitCode}) after {attempt} attempt(s); see output above");
    }
}
