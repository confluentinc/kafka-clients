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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// The in-suite perf smoke — the C# analog of <c>producer_performance_test.py::test_producer_e2e_latency</c>
/// and <c>consumer_performance_test.py::{test_consumer_e2e_latency, _async}</c>. It spins a KRaft broker
/// (<see cref="KafkaBrokerFixture"/>), then subprocess-invokes the built <b>PerfV3</b> exe BY PATH with the
/// shared budget config (100 rps / 10 s / p99 ≤ 70 ms / 2048-byte values), asserting a clean (exit 0) run.
/// </summary>
/// <remarks>
/// <para><b>v3-only gate (D10):</b> only PerfV3 is exercised here; the v2 (PerfV2/ckd) baseline is
/// manual-comparison only and is never part of this smoke.</para>
/// <para><b>Same-assembly collision (PLAN §1.1.1 / §4):</b> this project references neither PerfV3 nor
/// ckd — it launches the PerfV3 exe as an external process (<c>dotnet exec PerfV3.dll</c>) resolved by
/// path, so the two Confluent.Kafka assemblies never share a build graph.</para>
/// <para><b>Docker-gated:</b> when Docker is unavailable the fixture reports a skip and each test returns a
/// logged pass (xUnit 2.9.x has no dynamic <c>Assert.Skip</c>). The actual Docker RUN is CI-only in the
/// local dev env.</para>
/// </remarks>
public sealed class PerfV3SmokeTests : IClassFixture<KafkaBrokerFixture>
{
    // Shared in-suite budget (PLAN §3.6) — 100 rps, 10 s, no warmup, 2048-byte values, p99 <= 70 ms.
    private const string SmokeClientVersion = "3";

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public PerfV3SmokeTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    // Test methods return the Task directly (no await in the body) so xUnit1030 (no ConfigureAwait in a
    // test method) and CA2007 both stay satisfied; the private helpers keep ConfigureAwait(false).
    [Fact]
    public Task Producer_Smoke_Sync() => RunProducerSmokeAsync("producer-perf-smoke", asyncMode: false);

    [Fact]
    public Task Producer_Smoke_Async() => RunProducerSmokeAsync("producer-perf-smoke-async", asyncMode: true);

    [Fact]
    public Task Consumer_Smoke_Sync() => RunConsumerSmokeAsync("consumer-perf-smoke", asyncMode: false);

    [Fact]
    public Task Consumer_Smoke_Async() => RunConsumerSmokeAsync("consumer-perf-smoke-async", asyncMode: true);

    private async Task RunProducerSmokeAsync(string topic, bool asyncMode)
    {
        if (!await _broker.TryStartAsync(_output).ConfigureAwait(false))
        {
            return; // Docker unavailable / broker start failed — skip cleanly (logged by the fixture).
        }

        await _broker.CreateTopicAsync(topic, partitions: 4).ConfigureAwait(false);

        // Matches the Rust in-suite config + Python test_producer_e2e_latency: 100 rps, 10 s, p99<=70 ms,
        // no warmup, 2048-byte values, no verify. CREATE_TOPIC=False (the fixture created it).
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
            ["P99_LIMIT_MS"] = "70",
            ["DO_VERIFY"] = "False",
            ["CREATE_TOPIC"] = "False",
        };

        (int exitCode, string stdout, string stderr) = await RunPerfV3Async(env, removeKeys: Array.Empty<string>()).ConfigureAwait(false);
        _output.WriteLine(stdout);
        _output.WriteLine(stderr);
        Assert.True(exitCode == 0, $"producer perf run failed (rc={exitCode}); see output above");
    }

    private async Task RunConsumerSmokeAsync(string topic, bool asyncMode)
    {
        if (!await _broker.TryStartAsync(_output).ConfigureAwait(false))
        {
            return; // Docker unavailable / broker start failed — skip cleanly (logged by the fixture).
        }

        await _broker.CreateTopicAsync(topic, partitions: 4).ConfigureAwait(false);

        // A steady ~100 msg/s in-container producer feeds CreateTime-stamped records for the consumer's
        // lifetime (conftest.produce_perf_in_container: 10000 records, 2048 bytes, 100 msg/s).
        await _broker.ProducePerfInContainerAsync(topic, numRecords: 10000, recordSize: 2048, throughput: 100).ConfigureAwait(false);

        // Matches consumer_performance_test.py::_smoke_env: 10 s, p99<=70 ms, no warmup, FETCH_MIN_BYTES=1
        // (so low-rate fetches return immediately), short join/settle. KAFKA_BIN removed (load comes from
        // the container). CREATE_TOPIC=False (the fixture created it).
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
            ["P99_LIMIT_MS"] = "70",
            ["JOIN_TIMEOUT_SECONDS"] = "60",
            ["SETTLE_TIMEOUT_SECONDS"] = "5",
            ["CREATE_TOPIC"] = "False",
        };

        (int exitCode, string stdout, string stderr) = await RunPerfV3Async(env, removeKeys: new[] { "KAFKA_BIN" }).ConfigureAwait(false);
        _output.WriteLine(stdout);
        _output.WriteLine(stderr);
        Assert.True(exitCode == 0, $"consumer perf run failed (rc={exitCode}); see output above");
    }

    /// <summary>
    /// Launches the built PerfV3 exe as <c>dotnet exec PerfV3.dll</c> with the given env overrides, in a
    /// temp working dir (so <c>metrics.jsonl</c> / <c>results.json</c> don't pollute the repo), and returns
    /// its exit code + captured output. Mirrors the Python tests' subprocess re-invoke (180 s timeout).
    /// </summary>
    private async Task<(int ExitCode, string Stdout, string Stderr)> RunPerfV3Async(
        IReadOnlyDictionary<string, string> env, IReadOnlyCollection<string> removeKeys)
    {
        string perfV3Dll = ResolvePerfV3Dll();
        Assert.True(File.Exists(perfV3Dll),
            $"PerfV3 exe not found at '{perfV3Dll}'. Build it first (make test-integration-perf-dotnet builds it), " +
            "or set PERFV3_DLL to its path.");

        // The dotnet host running this test (set by the SDK during `dotnet test`); falls back to PATH.
        string dotnetHost = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";

        string workDir = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "perfv3-smoke-" + Guid.NewGuid().ToString("N"))).FullName;

        var startInfo = new ProcessStartInfo(dotnetHost)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = workDir,
        };
        startInfo.ArgumentList.Add("exec");
        startInfo.ArgumentList.Add(perfV3Dll);

        // startInfo.Environment is pre-populated with this process's env; layer the budget overrides on top.
        foreach (KeyValuePair<string, string> kv in env)
        {
            startInfo.Environment[kv.Key] = kv.Value;
        }

        foreach (string key in removeKeys)
        {
            startInfo.Environment.Remove(key);
        }

        using var process = new Process { StartInfo = startInfo };
        process.Start();

        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync();
        Task<string> stderrTask = process.StandardError.ReadToEndAsync();

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (Exception)
            {
                // Best-effort kill on timeout.
            }

            throw new TimeoutException($"PerfV3 did not exit within the smoke timeout ({perfV3Dll}).");
        }

        string stdout = await stdoutTask.ConfigureAwait(false);
        string stderr = await stderrTask.ConfigureAwait(false);

        TryDeleteDirectory(workDir);
        return (process.ExitCode, stdout, stderr);
    }

    /// <summary>
    /// Resolves the built PerfV3 DLL by path: the <c>PERFV3_DLL</c> env override if set, else the sibling
    /// <c>PerfV3/bin/&lt;Config&gt;/&lt;Tfm&gt;/PerfV3.dll</c> — reusing THIS test's own config + TFM (its
    /// <see cref="AppContext.BaseDirectory"/> ends in <c>.../bin/&lt;Config&gt;/&lt;Tfm&gt;/</c>) so the
    /// launched exe matches the running test's build.
    /// </summary>
    private static string ResolvePerfV3Dll()
    {
        string? overridePath = Environment.GetEnvironmentVariable("PERFV3_DLL");
        if (!string.IsNullOrEmpty(overridePath))
        {
            return overridePath!;
        }

        string baseDir = AppContext.BaseDirectory
            .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        // baseDir = <PerfRoot>/Confluent.Kafka.PerformanceTests/bin/<Config>/<Tfm>
        string tfm = Path.GetFileName(baseDir);
        var configDir = Directory.GetParent(baseDir)!;
        string config = configDir.Name;

        // configDir.Parent = bin; .Parent = the test project dir; .Parent = <PerfRoot> (tests/Performance).
        DirectoryInfo perfRoot = configDir.Parent!.Parent!.Parent!;
        return Path.Combine(perfRoot.FullName, "PerfV3", "bin", config, tfm, "PerfV3.dll");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception)
        {
            // Best-effort cleanup of the temp working dir.
        }
    }
}
