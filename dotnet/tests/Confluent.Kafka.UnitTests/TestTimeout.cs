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
using System.Threading.Tasks;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Runs a blocking action under a hard deadline so a native call that hangs (e.g.
/// a broker-less close that never returns) <b>fails fast</b> as a test failure
/// rather than hanging the run — the completion/teardown regression guard the ffi
/// rules ask for (ffi §0.3, §B7 "Dispose … doesn't hang").
/// </summary>
internal static class TestTimeout
{
    /// <summary>
    /// Runs <paramref name="action"/> on the thread pool and waits up to
    /// <paramref name="timeout"/>. Throws <see cref="TimeoutException"/> if it does
    /// not complete in time; otherwise rethrows any exception the action produced.
    /// </summary>
    internal static void Run(Action action, TimeSpan timeout)
    {
        Task task = Task.Run(action);
        if (!task.Wait(timeout))
        {
            throw new TimeoutException(
                $"Operation did not complete within {timeout} — treated as a hang (fail fast).");
        }

        // Surface any exception thrown by the action on the caller's thread.
        task.GetAwaiter().GetResult();
    }

    /// <summary>
    /// Awaits <paramref name="action"/>'s <see cref="Task"/> under a hard deadline.
    /// Throws <see cref="TimeoutException"/> if it does not complete in time (the
    /// async completion-bridge / teardown hang guard, ffi §B7); otherwise the
    /// action's own result / exception is observed by the final <c>await</c>. Every
    /// awaited op and teardown in the async tests routes through this so a bridge or
    /// drain hang fails the run fast instead of blocking it.
    /// </summary>
    internal static async Task Run(Func<Task> action, TimeSpan timeout)
    {
        Task task = action();
        Task winner = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException(
                $"Operation did not complete within {timeout} — treated as a hang (fail fast).");
        }

        // Surface the action's result / exception.
        await task.ConfigureAwait(false);
    }

    /// <summary>
    /// The value-returning twin of <see cref="Run(Func{Task}, TimeSpan)"/>, so a test can
    /// read an awaited result without the assign-inside-a-lambda dance. Same fail-fast
    /// behaviour: a hang becomes a <see cref="TimeoutException"/>, and the action's own
    /// exception is surfaced by the final <c>await</c>.
    /// </summary>
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
}
