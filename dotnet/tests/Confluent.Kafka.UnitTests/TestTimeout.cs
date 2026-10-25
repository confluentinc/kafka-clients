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
using System.Linq;
using System.Threading.Tasks;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Runs a blocking or async action under a hard deadline, so a hang fails the test with a
/// <see cref="TimeoutException"/> instead of hanging the run. <see cref="RunOffThisThread"/> starts an async action on
/// a pool thread, so a call that blocks before it returns its Task is bounded too. The teardown helpers bound a
/// client's <c>Dispose</c>, <c>DisposeAsync</c> or <c>Close</c> the same way, under <see cref="TeardownBound"/>, and
/// name the client's type when it hangs; <see cref="Disposal"/> and <see cref="AsyncDisposal"/> stand in for a
/// <c>using</c> or <c>await using</c> declaration.
/// </summary>
internal static class TestTimeout
{
    internal static readonly TimeSpan TeardownBound = TimeSpan.FromSeconds(60);

    internal static void Run(Action action, TimeSpan timeout)
    {
        Task task = Task.Run(action);
        if (!task.Wait(timeout))
        {
            throw new TimeoutException(
                $"Operation did not complete within {timeout} — treated as a hang (fail fast).");
        }

        task.GetAwaiter().GetResult();
    }

    internal static async Task Run(Func<Task> action, TimeSpan timeout)
    {
        Task task = action();
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException(
                $"Operation did not complete within {timeout} — treated as a hang (fail fast).");
        }

        await task.ConfigureAwait(false);
    }

    internal static Task RunOffThisThread(Func<Task> action, TimeSpan timeout) => Run(() => Task.Run(action), timeout);

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
        Task task = Task.Run(teardown);
        if (!((IAsyncResult)task).AsyncWaitHandle.WaitOne(timeout))
        {
            throw Hung(target, timeout);
        }

        task.GetAwaiter().GetResult();
    }

    internal static async Task TeardownAsync(object target, Func<Task> teardown, TimeSpan? bound = null)
    {
        TimeSpan timeout = bound ?? TeardownBound;
        Task task = Task.Run(teardown);
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw Hung(target, timeout);
        }

        await task.ConfigureAwait(false);
    }

    internal static async Task<TResult> Run<TResult>(Func<Task<TResult>> action, TimeSpan timeout)
    {
        Task<TResult> task = action();
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException(
                $"Operation did not complete within {timeout} — treated as a hang (fail fast).");
        }

        return await task.ConfigureAwait(false);
    }

    private static TimeoutException Hung(object target, TimeSpan timeout) =>
        new TimeoutException(
            $"Teardown of {NameOf(target.GetType())} did not complete within {timeout} — treated as a hang (fail fast).");

    private static string NameOf(Type type) =>
        type.IsGenericType
            ? type.Name.Substring(0, type.Name.IndexOf('`')) + "<" + string.Join(", ", type.GetGenericArguments().Select(NameOf)) + ">"
            : type.Name;

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
