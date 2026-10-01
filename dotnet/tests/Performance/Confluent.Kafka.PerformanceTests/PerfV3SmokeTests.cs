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
/// <para><b>Docker-gated, visibly:</b> when Docker is genuinely unavailable each smoke reports a real
/// xUnit <c>Skipped</c> (via <c>Xunit.SkippableFact</c>, M13/P3 D-8 Option B) — matching
/// <c>pytest.skip</c> in the Python suite. When <c>PERF_REQUIRE_DOCKER=True</c> (which the
/// <c>test-integration-perf-dotnet</c> Makefile target, and therefore CI, sets) a missing or broken
/// Docker is a FAILURE instead, because Docker was supposed to be there.</para>
/// </remarks>
public sealed class PerfV3SmokeTests : IClassFixture<KafkaBrokerFixture>
{
    // Shared in-suite budget (PLAN §3.6) — 100 rps, 10 s, no warmup, 2048-byte values, p99 <= 70 ms.
    private const string SmokeClientVersion = "3";

    // Consumer e2e latency is (host consumer clock) - (producer CreateTime), so it is only meaningful
    // when the feeder and the consumer share a clock. On macOS the feeder runs inside the Docker VM,
    // and macOS gives no GUARANTEE that the VM shares the host clock: Colima drifts, and a
    // drifted-forward record yields a NEGATIVE latency, which the engine treats as unmeasurable and
    // drops — so it drops every record, measures nothing, and fails with "FAIL: no messages measured"
    // while pointing at the client. On Linux the broker container does share the host clock.
    //
    // Be precise about what this skip costs, because M13/P3 measured it: Docker Desktop on macOS
    // currently DOES share the host clock (`date -u +%s` returns the same second inside and outside a
    // container), and both consumer smokes passed on such a host before this skip existed. The skip is
    // therefore platform-based and unconditional — for parity with consumer_performance_test.py's
    // _SKIP_CROSS_CLOCK (`sys.platform == "darwin"`), which is likewise not drift-detected — and on a
    // clock-synchronised macOS host it costs two smokes that would otherwise pass. It buys immunity to
    // the Colima case, where the failure is real and its message is actively misleading. A
    // drift-probing variant would keep that coverage but was not among D-7's options, is not Python
    // parity, and adds a probe that can itself flake.
    //
    // Granularity matches Python's: the PRODUCER smokes measure send->ack on one clock, are unaffected,
    // and keep running on macOS — so this is an in-test skip on the two consumer smokes only, NOT a
    // Makefile-target guard (D-7 Option A). PERF_REQUIRE_DOCKER deliberately does NOT override it — a
    // platform truth is not an infrastructure failure — which is why it is checked BEFORE the Docker
    // gate.
    private const string CrossClockReason =
        "consumer e2e latency needs the feeder and consumer to share a clock, which macOS does not " +
        "guarantee across the Docker VM boundary (Colima drifts; Docker Desktop currently does not). " +
        "Skipped unconditionally on macOS for parity with the Python suite; runs on Linux, where the " +
        "broker container shares the host clock";

    private static readonly bool s_skipCrossClock = OperatingSystem.IsMacOS();

    // Item G: how long a timed-out run's output readers get to finish before we give up on them. Bounded
    // so a wedged pipe cannot turn a timeout into a hang.
    private static readonly TimeSpan s_capturedDrainTimeout = TimeSpan.FromSeconds(5);

    // How much of the timed-out run's stderr rides along in the TimeoutException message, so the headline
    // is diagnostic even where only the exception is quoted.
    private const int StderrTailChars = 2000;

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public PerfV3SmokeTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    // Test methods return the Task directly (no await in the body) so xUnit1030 (no ConfigureAwait in a
    // test method) and CA2007 both stay satisfied; the private helpers keep ConfigureAwait(false).
    // [SkippableFact] (not [Fact]) so a Skip.If raised inside those helpers — Docker genuinely absent,
    // or the macOS cross-clock boundary — surfaces as a real Skipped result instead of a silent pass.
    // It supports async Task methods, so the signatures are unchanged. Crc32Test stays a plain
    // [Theory]: it is pure arithmetic and can never legitimately skip.
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
            // Inherit a looser P99_LIMIT_MS if one is set (e.g. a macOS run); 0 disables the latency
            // assert, and the 70 ms default keeps the Linux budget unchanged. Read via PerfEnv because
            // this dictionary is layered ON TOP of the inherited environment in RunPerfV3Async — a
            // hardcoded "70" would silently overwrite whatever the caller set. Only P99_LIMIT_MS gets
            // this treatment: it is the only variable Python passes through, and widening it further
            // would quietly change what the gate measures.
            ["P99_LIMIT_MS"] = PerfEnv.GetString("P99_LIMIT_MS", "70"),
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
        // BEFORE EnsureStartedAsync on purpose (see CrossClockReason): a macOS run under CI-style
        // settings must still skip, not be converted into a Docker failure by PERF_REQUIRE_DOCKER.
        Skip.If(s_skipCrossClock, CrossClockReason);

        await _broker.EnsureStartedAsync(_output).ConfigureAwait(false);
        await _broker.CreateTopicAsync(topic, partitions: 4).ConfigureAwait(false);

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
            // Same inherited-P99_LIMIT_MS rule as the producer smoke above.
            ["P99_LIMIT_MS"] = PerfEnv.GetString("P99_LIMIT_MS", "70"),
            ["JOIN_TIMEOUT_SECONDS"] = "60",
            ["SETTLE_TIMEOUT_SECONDS"] = "5",
            ["CREATE_TOPIC"] = "False",
        };

        // Mirrors consumer_performance_test.py::_run_consumer_smoke: for up to PERF_SETUP_ATTEMPTS
        // tries, drive a steady ~100 msg/s in-container producer (conftest.produce_perf_in_container:
        // 10000 records, 2048 bytes, 100 msg/s) and run the benchmark. A SETUP_NOT_READY exit means the
        // pipeline never went live (a slow feeder under a virtualized Docker host), so the feeder is
        // restarted and the run retried rather than failed.
        int attempts = PerfEnv.GetInt("PERF_SETUP_ATTEMPTS", 3);

        // The knob must not be able to express "run nothing". At attempts < 1 the loop body never
        // executes: no feeder starts, RunPerfV3Async is never called, the File.Exists guard on the
        // PerfV3 exe is never reached — and a smoke that measured nothing would report Passed. That is
        // item E's own silent-pass failure mode arriving through the retry knob item A just added, so
        // it is rejected at the input rather than patched at the initialiser. Python cannot express it
        // either: _run_consumer_smoke initialises proc = None and asserts on proc.returncode, so
        // attempts = 0 raises AttributeError — a test ERROR, never a pass.
        Assert.True(
            attempts >= 1,
            $"PERF_SETUP_ATTEMPTS must be at least 1 (got {attempts}); a lower value would run no attempt " +
            "at all and report a pass having measured nothing.");

        // -1, not 0: "no result yet" must never be spelled the same way as "the run succeeded". This is
        // the C# stand-in for Python's `proc = None` sentinel. Unreachable given the guard above, and
        // deliberately so — it is the second line of defence, not the first.
        int exitCode = -1;
        int attempt = 0;
        for (attempt = 1; attempt <= attempts; attempt++)
        {
            await _broker.ProducePerfInContainerAsync(topic, numRecords: 10000, recordSize: 2048, throughput: 100).ConfigureAwait(false);

            string stdout;
            string stderr;
            try
            {
                (exitCode, stdout, stderr) = await RunPerfV3Async(env, removeKeys: new[] { "KAFKA_BIN" }).ConfigureAwait(false);
            }
            finally
            {
                // D-2 (deliberate divergence from Python, whose stop() is a no-op): actually stop this
                // attempt's feeder, so a retry restarts one rather than stacking a second alongside it.
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

        try
        {
            return await RunPerfV3CoreAsync(env, removeKeys, dotnetHost, perfV3Dll, workDir).ConfigureAwait(false);
        }
        finally
        {
            // In a finally, NOT on the success path only: every timeout used to leave a
            // perfv3-smoke-<guid> directory behind in the system temp folder.
            TryDeleteDirectory(workDir);
        }
    }

    /// <summary>
    /// The launch + wait + capture body of <see cref="RunPerfV3Async"/>, split out so the temp working
    /// directory is cleaned up in that method's <c>finally</c> on both the success and timeout paths.
    /// </summary>
    private async Task<(int ExitCode, string Stdout, string Stderr)> RunPerfV3CoreAsync(
        IReadOnlyDictionary<string, string> env,
        IReadOnlyCollection<string> removeKeys,
        string dotnetHost,
        string perfV3Dll,
        string workDir)
    {
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

            // Item G: a hang is the most likely CI failure, and it used to produce the least useful
            // message of any — the two reader tasks were abandoned, so anything the benchmark printed
            // ("assigned after 45s", "no records within 120s", a stack trace) was discarded. Drain them
            // under a bounded wait and put the result in the test log.
            (string timedOutStdout, string timedOutStderr) =
                await DrainCapturedAsync(stdoutTask, stderrTask).ConfigureAwait(false);
            _output.WriteLine("--- PerfV3 stdout (timed-out run) ---");
            _output.WriteLine(timedOutStdout);
            _output.WriteLine("--- PerfV3 stderr (timed-out run) ---");
            _output.WriteLine(timedOutStderr);

            throw new TimeoutException(
                $"PerfV3 did not exit within the smoke timeout ({perfV3Dll}). stderr tail: {Tail(timedOutStderr, StderrTailChars)}");
        }

        string stdout = await stdoutTask.ConfigureAwait(false);
        string stderr = await stderrTask.ConfigureAwait(false);
        return (process.ExitCode, stdout, stderr);
    }

    /// <summary>
    /// Awaits the two already-running output readers under <see cref="s_capturedDrainTimeout"/> and
    /// returns whatever they captured. Never throws: a failure here must not replace the timeout the
    /// caller is reporting.
    /// </summary>
    private static async Task<(string Stdout, string Stderr)> DrainCapturedAsync(Task<string> stdoutTask, Task<string> stderrTask)
    {
        try
        {
            Task readers = Task.WhenAll(stdoutTask, stderrTask);
            Task finished = await Task.WhenAny(readers, Task.Delay(s_capturedDrainTimeout)).ConfigureAwait(false);
            if (!ReferenceEquals(finished, readers))
            {
                const string Wedged = "(not captured: the output readers did not finish after the process was killed)";
                return (Wedged, Wedged);
            }

            return (await stdoutTask.ConfigureAwait(false), await stderrTask.ConfigureAwait(false));
        }
        catch (Exception e)
        {
            return ($"(output capture failed: {e.Message})", string.Empty);
        }
    }

    /// <summary>Returns at most <paramref name="maxChars"/> trailing characters of <paramref name="value"/>.</summary>
    private static string Tail(string value, int maxChars)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= maxChars)
        {
            return value;
        }

        return "..." + value.Substring(value.Length - maxChars);
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
