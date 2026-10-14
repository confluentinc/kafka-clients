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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// One future of either flavour, observed without an unbounded wait: <see cref="IsDone"/>, a "still waiting" check
/// and its outcome — the value, <see cref="BlockingCall.Returned"/> for a void future that completed, or the exception
/// it threw. A sync future's <c>Get()</c> runs on a dedicated thread (<see cref="BlockingCall"/>); an async one's
/// <see cref="Task"/> is awaited under <see cref="TestTimeout"/>.
/// </summary>
internal abstract class FutureProbe
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    internal abstract bool IsDone { get; }

    internal static FutureProbe Of(KafkaFuture future) =>
        new SyncProbe(() => future.IsDone, () =>
        {
            future.Get();
            return BlockingCall.Returned;
        });

    internal static FutureProbe Of<T>(KafkaFuture<T> future) => new SyncProbe(() => future.IsDone, () => future.Get());

    internal static FutureProbe Of(AsyncKafkaFuture future) => new AsyncProbe(future.Get(), static async task =>
    {
        await task.ConfigureAwait(false);
        return BlockingCall.Returned;
    });

    internal static FutureProbe Of<T>(AsyncKafkaFuture<T> future) => new AsyncProbe(future.Get(), static async task =>
        await ((Task<T>)task).ConfigureAwait(false));

    /// <summary>Asserts the future's <c>Get()</c> would still be waiting.</summary>
    internal abstract void AssertPending();

    /// <summary>The future's outcome, within the deadline; each call observes the same future again.</summary>
    internal abstract Task<object?> Outcome();

    private sealed class SyncProbe : FutureProbe
    {
        private readonly Func<bool> _isDone;
        private readonly Func<object?> _get;
        private BlockingCall? _started;

        internal SyncProbe(Func<bool> isDone, Func<object?> get)
        {
            _isDone = isDone;
            _get = get;
        }

        internal override bool IsDone => _isDone();

        internal override void AssertPending()
        {
            _started ??= BlockingCall.StartGet(_get);
            _started.AssertBlocked();
        }

        internal override Task<object?> Outcome()
        {
            BlockingCall call = _started ?? BlockingCall.StartGet(_get);
            _started = null;
            return Task.FromResult(call.Join());
        }
    }

    private sealed class AsyncProbe : FutureProbe
    {
        private readonly Task _task;
        private readonly Func<Task, Task<object?>> _await;

        internal AsyncProbe(Task task, Func<Task, Task<object?>> await)
        {
            _task = task;
            _await = await;
        }

        internal override bool IsDone => _task.IsCompleted;

        internal override void AssertPending() => Assert.False(_task.IsCompleted, "the future had already completed");

        internal override async Task<object?> Outcome()
        {
            try
            {
                return await TestTimeout.Run(() => _await(_task), s_deadline).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not TimeoutException)
            {
                return exception;
            }
        }
    }
}
