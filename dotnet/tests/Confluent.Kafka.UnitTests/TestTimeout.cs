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
/// Runs a blocking or async action under a hard deadline, so a hang fails the test with a
/// <see cref="TimeoutException"/> instead of hanging the run.
/// </summary>
internal static class TestTimeout
{
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
