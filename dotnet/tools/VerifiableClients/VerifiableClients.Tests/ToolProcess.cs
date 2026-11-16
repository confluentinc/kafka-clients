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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// A built tool (the running test's configuration and TFM) run as a child process, <c>dotnet exec</c> on its
/// <c>.dll</c>, with its stdout read line by line and its stderr collected.
/// </summary>
internal sealed class ToolProcess : IDisposable
{
    private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(60);

    private readonly Process _process;

    private readonly Task<string> _stderr;

    private ToolProcess(Process process)
    {
        _process = process;
        _stderr = process.StandardError.ReadToEndAsync();
    }

    internal static ToolProcess Start(string tool, params string[] args)
    {
        string dll = ToolDll(tool);
        Assert.True(File.Exists(dll), $"{tool} not found at '{dll}': build the solution first.");
        var startInfo = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        startInfo.ArgumentList.Add("exec");
        startInfo.ArgumentList.Add(dll);
        foreach (string arg in args)
        {
            startInfo.ArgumentList.Add(arg);
        }

        return new ToolProcess(Process.Start(startInfo)!);
    }

    internal async Task<string> ReadLineAsync()
    {
        string? line = await _process.StandardOutput.ReadLineAsync().WaitAsync(s_timeout).ConfigureAwait(false);
        if (line is null)
        {
            Assert.Fail("The tool ended its output early: " + await StderrSoFarAsync().ConfigureAwait(false));
        }

        return line!;
    }

    internal void Signal(string signal)
    {
        var startInfo = new ProcessStartInfo("kill") { UseShellExecute = false };
        startInfo.ArgumentList.Add("-" + signal);
        startInfo.ArgumentList.Add(_process.Id.ToString(CultureInfo.InvariantCulture));
        using Process kill = Process.Start(startInfo)!;
        kill.WaitForExit();
        Assert.Equal(0, kill.ExitCode);
    }

    internal async Task<(int ExitCode, string Stdout, string Stderr)> WaitForExitAsync()
    {
        using var timeout = new CancellationTokenSource(s_timeout);
        Task<string> stdout = _process.StandardOutput.ReadToEndAsync();
        try
        {
            await _process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Kill();
            throw new global::System.TimeoutException(
                $"The tool did not exit within {s_timeout.TotalSeconds} s: " + await StderrSoFarAsync().ConfigureAwait(false));
        }

        return (_process.ExitCode, await stdout.ConfigureAwait(false), await _stderr.ConfigureAwait(false));
    }

    public void Dispose()
    {
        Kill();
        _process.Dispose();
    }

    private static string ToolDll(string tool)
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar));
        string tfm = baseDir.Name;
        DirectoryInfo configDir = baseDir.Parent!;
        DirectoryInfo tools = configDir.Parent!.Parent!.Parent!;
        return Path.Combine(tools.FullName, tool, "bin", configDir.Name, tfm, tool + ".dll");
    }

    private async Task<string> StderrSoFarAsync()
    {
        Kill();
        Task finished = await Task.WhenAny(_stderr, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
        string exit = _process.HasExited ? "exit code " + _process.ExitCode.ToString(CultureInfo.InvariantCulture) + ": " : string.Empty;
        return exit + (finished == _stderr ? await _stderr.ConfigureAwait(false) : "(stderr not captured)");
    }

    private void Kill()
    {
        try
        {
            if (!_process.HasExited)
            {
                _process.Kill(entireProcessTree: true);
            }
        }
        catch (InvalidOperationException)
        {
        }
    }
}

/// <summary>A test sending a signal with <c>kill</c>, which Windows does not have: skipped there.</summary>
internal sealed class UnixFactAttribute : FactAttribute
{
    public UnixFactAttribute()
    {
        if (OperatingSystem.IsWindows())
        {
            Skip = "Sends a POSIX signal with kill, which Windows does not have.";
        }
    }
}
