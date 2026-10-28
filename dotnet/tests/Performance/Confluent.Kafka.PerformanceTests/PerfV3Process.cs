// Copyright 2026 Confluent Inc.
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
/// Runs the built PerfV3 exe (the running test's configuration and TFM) as a child process with environment
/// overrides, and returns its exit code and output.
/// </summary>
internal static class PerfV3Process
{
    // Bounds the wait for a timed-out run's output readers, so a wedged pipe cannot turn a timeout into a hang.
    private static readonly TimeSpan s_capturedDrainTimeout = TimeSpan.FromSeconds(5);

    private const int StderrTailChars = 2000;

    internal static async Task<(int ExitCode, string Stdout, string Stderr)> RunAsync(
        ITestOutputHelper output, IReadOnlyDictionary<string, string> env, IReadOnlyCollection<string> removeKeys)
    {
        string perfV3Dll = ResolvePerfV3Dll();
        Assert.True(File.Exists(perfV3Dll),
            $"PerfV3 exe not found at '{perfV3Dll}'. Build it first (make test-integration-perf-dotnet builds it), " +
            "or set PERFV3_DLL to its path.");

        // Set by the SDK under dotnet test; else the dotnet on PATH.
        string dotnetHost = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";

        string workDir = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "perfv3-run-" + Guid.NewGuid().ToString("N"))).FullName;

        try
        {
            return await RunCoreAsync(output, env, removeKeys, dotnetHost, perfV3Dll, workDir).ConfigureAwait(false);
        }
        finally
        {
            TryDeleteDirectory(workDir);
        }
    }

    private static async Task<(int ExitCode, string Stdout, string Stderr)> RunCoreAsync(
        ITestOutputHelper output,
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

        // The child inherits this process's environment; the overrides go on top.
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

            // Drain the readers under a bounded wait, so what a hung run printed reaches the test log.
            (string timedOutStdout, string timedOutStderr) =
                await DrainCapturedAsync(stdoutTask, stderrTask).ConfigureAwait(false);
            output.WriteLine("--- PerfV3 stdout (timed-out run) ---");
            output.WriteLine(timedOutStdout);
            output.WriteLine("--- PerfV3 stderr (timed-out run) ---");
            output.WriteLine(timedOutStderr);

            throw new TimeoutException(
                $"PerfV3 did not exit within its 3-minute timeout ({perfV3Dll}). stderr tail: {Tail(timedOutStderr, StderrTailChars)}");
        }

        string stdout = await stdoutTask.ConfigureAwait(false);
        string stderr = await stderrTask.ConfigureAwait(false);
        return (process.ExitCode, stdout, stderr);
    }

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

    private static string Tail(string value, int maxChars)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= maxChars)
        {
            return value;
        }

        return "..." + value.Substring(value.Length - maxChars);
    }

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
