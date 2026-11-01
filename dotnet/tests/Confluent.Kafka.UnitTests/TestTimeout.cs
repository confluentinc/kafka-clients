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
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
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
/// and when, and the thread pool's and the process's thread counts, to tell a pool that never ran it from a hung call.
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
        $"Operation did not complete within {timeout} — treated as a hang (fail fast). {StartProbe.Describe(probe)}";

    private static TimeoutException Hung(object target, TimeSpan timeout, StartProbe probe) =>
        new TimeoutException(
            $"Teardown of {NameOf(target.GetType())} did not complete within {timeout} — treated as a hang (fail fast). "
            + StartProbe.Describe(probe));

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
