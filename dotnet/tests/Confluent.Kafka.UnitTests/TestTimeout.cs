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
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Runs a blocking or async action under a hard deadline, so a hang fails the test with a
/// <see cref="TimeoutException"/> instead of hanging the run. <see cref="RunOffThisThread"/> starts an async action on
/// a pool thread, so a call that blocks before it returns its Task is bounded too. The teardown helpers bound a
/// client's <c>Dispose</c>, <c>DisposeAsync</c> or <c>Close</c> the same way, under <see cref="TeardownBound"/>, and
/// name the client's type when it hangs; <see cref="Disposal"/> and <see cref="AsyncDisposal"/> stand in for a
/// <c>using</c> or <c>await using</c> declaration. A timeout's message ends with diagnostics: whether the action started
/// and when, and the thread pool's and the process's thread counts, to tell a pool that never ran it from a hung call;
/// on macOS CI it also ends with the native stacks of the threads inside the native library (<see cref="NativeStacks"/>).
/// </summary>
internal static class TestTimeout
{
    internal static readonly TimeSpan TeardownBound = TimeSpan.FromSeconds(60);

    internal static void Run(Action action, TimeSpan timeout)
    {
        StartProbe probe = new StartProbe();
        Task task = Task.Run(
            () =>
            {
                probe.Start();
                action();
            });
        if (!task.Wait(timeout))
        {
            throw new TimeoutException(TimedOut(timeout, probe));
        }

        task.GetAwaiter().GetResult();
    }

    internal static Task Run(Func<Task> action, TimeSpan timeout) => RunBounded(action, timeout, probe: null);

    internal static Task RunOffThisThread(Func<Task> action, TimeSpan timeout)
    {
        StartProbe probe = new StartProbe();
        return RunBounded(
            () => Task.Run(
                () =>
                {
                    probe.Start();
                    return action();
                }),
            timeout,
            probe);
    }

    internal static void Dispose(IDisposable? target, TimeSpan? bound = null)
    {
        if (target is not null)
        {
            Teardown(target, target.Dispose, bound);
        }
    }

    internal static Task DisposeAsync(IAsyncDisposable? target, TimeSpan? bound = null) =>
        target is null ? Task.CompletedTask : TeardownAsync(target, () => target.DisposeAsync().AsTask(), bound);

    internal static IDisposable Disposal(IDisposable? target, TimeSpan? bound = null) => new BoundedDisposal(target, bound);

    internal static IAsyncDisposable AsyncDisposal(IAsyncDisposable? target, TimeSpan? bound = null) =>
        new BoundedAsyncDisposal(target, bound);

    internal static void Teardown(object target, Action teardown, TimeSpan? bound = null)
    {
        TimeSpan timeout = bound ?? TeardownBound;
        StartProbe probe = new StartProbe();
        Task task = Task.Run(
            () =>
            {
                probe.Start();
                teardown();
            });
        if (!((IAsyncResult)task).AsyncWaitHandle.WaitOne(timeout))
        {
            throw Hung(target, timeout, probe);
        }

        task.GetAwaiter().GetResult();
    }

    internal static async Task TeardownAsync(object target, Func<Task> teardown, TimeSpan? bound = null)
    {
        TimeSpan timeout = bound ?? TeardownBound;
        StartProbe probe = new StartProbe();
        Task task = Task.Run(
            () =>
            {
                probe.Start();
                return teardown();
            });
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw Hung(target, timeout, probe);
        }

        await task.ConfigureAwait(false);
    }

    internal static async Task<TResult> Run<TResult>(Func<Task<TResult>> action, TimeSpan timeout)
    {
        Task<TResult> task = action();
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException(TimedOut(timeout, probe: null));
        }

        return await task.ConfigureAwait(false);
    }

    private static async Task RunBounded(Func<Task> action, TimeSpan timeout, StartProbe? probe)
    {
        Task task = action();
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException(TimedOut(timeout, probe));
        }

        await task.ConfigureAwait(false);
    }

    private static string TimedOut(TimeSpan timeout, StartProbe? probe) =>
        $"Operation did not complete within {timeout} — treated as a hang (fail fast). {StartProbe.Describe(probe)}"
        + NativeStacks.Capture(timeout);

    private static TimeoutException Hung(object target, TimeSpan timeout, StartProbe probe) =>
        new TimeoutException(
            $"Teardown of {NameOf(target.GetType())} did not complete within {timeout} — treated as a hang (fail fast). "
            + StartProbe.Describe(probe) + NativeStacks.Capture(timeout));

    private static string NameOf(Type type) =>
        type.IsGenericType
            ? type.Name.Substring(0, type.Name.IndexOf('`')) + "<" + string.Join(", ", type.GetGenericArguments().Select(NameOf)) + ">"
            : type.Name;

    /// <summary>
    /// Records when a bounded action started on the thread pool, and describes it, with the pool's and the process's
    /// thread counts, when the bound expires.
    /// </summary>
    internal sealed class StartProbe
    {
        private readonly Stopwatch _sinceQueued = Stopwatch.StartNew();
        private long _startedAfterMs = -1;

        internal void Start() => Interlocked.CompareExchange(ref _startedAfterMs, _sinceQueued.ElapsedMilliseconds, -1);

        internal static string Describe(StartProbe? probe)
        {
            if (probe is null)
            {
                return $"Diagnostics: {PoolAndProcess()}.";
            }

            long started = Interlocked.Read(ref probe._startedAfterMs);
            long now = probe._sinceQueued.ElapsedMilliseconds;
            string action = started < 0
                ? $"the action never started in {now} ms"
                : $"the action started {started} ms after it was queued and ran {now - started} ms";
            return $"Diagnostics: {action}; {PoolAndProcess()}.";
        }

        private static string PoolAndProcess()
        {
            ThreadPool.GetAvailableThreads(out int availableWorkers, out _);
            ThreadPool.GetMaxThreads(out int maxWorkers, out _);
            ThreadPool.GetMinThreads(out int minWorkers, out _);
#if NETCOREAPP3_0_OR_GREATER
            string counts = $"{ThreadPool.ThreadCount} threads, {ThreadPool.PendingWorkItemCount} queued work items, ";
#else
            string counts = string.Empty;
#endif
            return $"thread pool: {counts}{maxWorkers - availableWorkers} of {maxWorkers} workers busy, minimum {minWorkers}; "
                + $"process: {OsThreads()} threads, {Environment.ProcessorCount} processors";
        }

        private static string OsThreads()
        {
            try
            {
                using Process self = Process.GetCurrentProcess();
                return $"{self.Threads.Count}";
            }
            catch (Exception e) when (e is NotSupportedException or InvalidOperationException or Win32Exception
                or UnauthorizedAccessException)
            {
                return "unknown";
            }
        }
    }

    /// <summary>
    /// Temporary, until the cause of the macOS CI teardown stalls is found: when a bound of at least
    /// <see cref="MinimumBound"/> expires on macOS and <see cref="DirectoryVariable"/> names a folder, samples the
    /// native stacks of every thread of this process with <c>/usr/bin/sample</c>, writes them to that folder, and
    /// summarizes the threads inside the native library.
    /// </summary>
    internal static class NativeStacks
    {
        internal const string DirectoryVariable = "CONFLUENT_KAFKA_DOTNET_HANG_DIR";
        internal static readonly TimeSpan MinimumBound = TimeSpan.FromSeconds(5);
        private const string Sampler = "/usr/bin/sample";
        private const string Library = "(in libconfluent_kafka.dylib)";
        private const int MaxSamples = 3;
        private const int DeepestFrames = 12;
        private const int MaxSummaryLines = 150;
        private static readonly Regex s_threadHeader = new Regex(@"^ {4}\d+ Thread_", RegexOptions.Compiled);
        private static readonly Regex s_address = new Regex(@"\s+\+ \d+\s+\[0x[0-9a-f]+\]$|\s+\[0x[0-9a-f]+\]$", RegexOptions.Compiled);
        private static readonly Regex s_waiting = new Regex(@"close|destroy|teardown|block_on|dispose", RegexOptions.Compiled | RegexOptions.IgnoreCase);
#if NET5_0_OR_GREATER
        private static readonly TimeSpan s_samplerBound = TimeSpan.FromSeconds(90);
        private static int s_taken;
#endif

        internal static bool ShouldSample(TimeSpan bound, string? directory, bool isMacOs) =>
            isMacOs && bound >= MinimumBound && !string.IsNullOrEmpty(directory);

        internal static string Capture(TimeSpan bound)
        {
#if NET5_0_OR_GREATER
            string? directory = Environment.GetEnvironmentVariable(DirectoryVariable);
            if (!ShouldSample(bound, directory, OperatingSystem.IsMacOS()) || !File.Exists(Sampler)
                || Interlocked.Increment(ref s_taken) > MaxSamples)
            {
                return string.Empty;
            }

            try
            {
                Directory.CreateDirectory(directory!);
                string file = Path.Combine(
                    directory!,
                    $"sample-{Environment.ProcessId}-{DateTime.UtcNow.ToString("yyyyMMdd'T'HHmmss.fff", CultureInfo.InvariantCulture)}.txt");
                ProcessStartInfo start = new ProcessStartInfo(Sampler)
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                };
                foreach (string argument in new[] { $"{Environment.ProcessId}", "2", "-mayDie", "-file", file })
                {
                    start.ArgumentList.Add(argument);
                }

                using Process sampler = Process.Start(start)!;
                Task<string> output = sampler.StandardOutput.ReadToEndAsync();
                Task<string> errors = sampler.StandardError.ReadToEndAsync();
                if (!sampler.WaitForExit((int)s_samplerBound.TotalMilliseconds))
                {
                    sampler.Kill();
                    return $" Native stacks: {Sampler} did not finish within {s_samplerBound}.";
                }

                if (sampler.ExitCode != 0 || !File.Exists(file))
                {
                    return $" Native stacks: {Sampler} exited with {sampler.ExitCode}: {errors.Result.Trim()} {output.Result.Trim()}";
                }

                return $"{Environment.NewLine}Native stacks (macOS sample, full file {file}):{Environment.NewLine}"
                    + Summarize(File.ReadAllLines(file));
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException or Win32Exception
                or InvalidOperationException or NotSupportedException)
            {
                return $" Native stacks: {Sampler} failed: {e.Message}";
            }
#else
            return string.Empty;
#endif
        }

        /// <summary>
        /// Keeps the threads of a <c>sample</c> call graph that run inside the native library, the ones whose stack
        /// mentions a close, a destroy or a wait on the runtime first, each as its header and its deepest frames.
        /// </summary>
        internal static string Summarize(IReadOnlyList<string> lines)
        {
            List<List<string>> threads = new List<List<string>>();
            bool inGraph = false;
            foreach (string line in lines)
            {
                if (line.StartsWith("Call graph:", StringComparison.Ordinal))
                {
                    inGraph = true;
                    continue;
                }

                if (!inGraph)
                {
                    continue;
                }

                if (line.Length == 0 || line.StartsWith("Total number in stack", StringComparison.Ordinal))
                {
                    break;
                }

                if (s_threadHeader.IsMatch(line))
                {
                    threads.Add(new List<string> { line.Trim() });
                }
                else if (threads.Count > 0)
                {
                    threads[threads.Count - 1].Add(line);
                }
            }

            IEnumerable<List<string>> inLibrary = threads
                .Where(thread => thread.Any(frame => frame.IndexOf(Library, StringComparison.Ordinal) >= 0))
                .OrderBy(thread => thread.Any(frame => s_waiting.IsMatch(frame)) ? 0 : 1);
            List<string> summary = new List<string>();
            foreach (List<string> thread in inLibrary)
            {
                summary.Add(thread[0]);
                summary.AddRange(thread.Skip(Math.Max(1, thread.Count - DeepestFrames)).Select(Frame));
                if (summary.Count >= MaxSummaryLines)
                {
                    summary.Add("(summary cut; see the full file)");
                    break;
                }
            }

            return summary.Count == 0 ? "(no thread inside the native library)" : string.Join(Environment.NewLine, summary);
        }

        private static string Frame(string line) => "  " + s_address.Replace(line.TrimStart(' ', '+', '!', ':', '|'), string.Empty);
    }

    /// <summary>The <see cref="Disposal"/> stand-in: its <c>Dispose</c> bounds the target's <c>Dispose</c>.</summary>
    private sealed class BoundedDisposal : IDisposable
    {
        private readonly IDisposable? _target;
        private readonly TimeSpan? _bound;

        internal BoundedDisposal(IDisposable? target, TimeSpan? bound)
        {
            _target = target;
            _bound = bound;
        }

        public void Dispose() => TestTimeout.Dispose(_target, _bound);
    }

    /// <summary>
    /// The <see cref="AsyncDisposal"/> stand-in: its <c>DisposeAsync</c> bounds the target's <c>DisposeAsync</c>.
    /// </summary>
    private sealed class BoundedAsyncDisposal : IAsyncDisposable
    {
        private readonly IAsyncDisposable? _target;
        private readonly TimeSpan? _bound;

        internal BoundedAsyncDisposal(IAsyncDisposable? target, TimeSpan? bound)
        {
            _target = target;
            _bound = bound;
        }

        public ValueTask DisposeAsync() => new ValueTask(TestTimeout.DisposeAsync(_target, _bound));
    }
}
