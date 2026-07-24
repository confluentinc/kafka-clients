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

namespace Confluent.Kafka.ShareConsumer.UnitTests;

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
}
