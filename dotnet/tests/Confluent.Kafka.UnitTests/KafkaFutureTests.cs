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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>KafkaFutureTest</c> run against <see cref="KafkaFuture{T}"/> and <see cref="KafkaFuture"/>; each
/// blocking <c>Get</c> runs on a thread of its own, so the shared tests can bound it.
/// </summary>
public sealed class KafkaFutureTests : KafkaFutureTestBase
{
    private protected override bool WaitsRethrowTheCancellation => true;

    [Fact(Skip = "KafkaFuture hands out no Task, so there is no completion stage to leak; AsyncKafkaFutureTests runs this test.")]
    public void TestLeakCompletableFuture()
    {
    }

    private protected override FutureView<T> Wrap<T>(KafkaFutureImpl<T> impl) => new View<T>(new KafkaFuture<T>(impl));

    private protected override FutureView<T> CompletedFuture<T>(T value) => new View<T>(KafkaFuture.CompletedFuture(value));

    private protected override FutureView AllOf(params FutureView<int>[] futures)
    {
        var erased = new KafkaFuture[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            erased[i] = ((View<int>)futures[i]).Future;
        }

        return new ErasedView(KafkaFuture.AllOf(erased));
    }

    private static Task<TR> OnThread<TR>(Func<TR> wait)
    {
        var result = new TaskCompletionSource<TR>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            try
            {
                result.SetResult(wait());
            }
            catch (Exception error)
            {
                result.SetException(error);
            }
        });
        thread.IsBackground = true;
        thread.Start();
        return result.Task;
    }

    /// <summary>A <see cref="KafkaFuture{T}"/> as the shared tests see it.</summary>
    private sealed class View<T> : FutureView<T>
    {
        private static readonly FieldInfo s_impl =
            typeof(KafkaFuture<T>).GetField("_impl", BindingFlags.NonPublic | BindingFlags.Instance)!;

        internal View(KafkaFuture<T> future) => Future = future;

        internal KafkaFuture<T> Future { get; }

        internal override KafkaFutureImpl<T> Impl => (KafkaFutureImpl<T>)s_impl.GetValue(Future)!;

        internal override bool IsDone => Future.IsDone;

        internal override bool IsCancelled => Future.IsCancelled;

        internal override bool IsCompletedExceptionally => Future.IsCompletedExceptionally;

        internal override bool Cancel(bool mayInterruptIfRunning) => Future.Cancel(mayInterruptIfRunning);

        internal override Task<T> Get()
        {
            KafkaFuture<T> future = Future;
            return OnThread(() => future.Get());
        }

        internal override Task<T> Get(TimeSpan timeout)
        {
            KafkaFuture<T> future = Future;
            return OnThread(() => future.Get(timeout));
        }

        internal override Task<T> Get(CancellationToken cancellationToken)
        {
            KafkaFuture<T> future = Future;
            return OnThread(() => future.Get(cancellationToken));
        }

        internal override T GetNow(T valueIfAbsent) => Future.GetNow(valueIfAbsent);

        internal override FutureView<TR> ThenApply<TR>(Func<T, TR> function) =>
            new View<TR>(Future.ThenApply<TR>(value => function(value)));

        internal override FutureView<T> WhenComplete(Action<T?, Exception?> action) =>
            new View<T>(Future.WhenComplete((value, error) => action(value, error)));

        internal override string Describe() => Future.ToString();
    }

    /// <summary>A <see cref="KafkaFuture"/> as the shared tests see it.</summary>
    private sealed class ErasedView : FutureView
    {
        private readonly KafkaFuture _future;

        internal ErasedView(KafkaFuture future) => _future = future;

        internal override bool IsDone => _future.IsDone;

        internal override bool IsCancelled => _future.IsCancelled;

        internal override bool IsCompletedExceptionally => _future.IsCompletedExceptionally;

        internal override bool Cancel(bool mayInterruptIfRunning) => _future.Cancel(mayInterruptIfRunning);

        internal override Task Wait()
        {
            KafkaFuture future = _future;
            return OnThread(() =>
            {
                future.Get();
                return true;
            });
        }

        internal override Task Wait(TimeSpan timeout)
        {
            KafkaFuture future = _future;
            return OnThread(() =>
            {
                future.Get(timeout);
                return true;
            });
        }

        internal override void WaitNow() => _future.GetNow();

        internal override string Describe() => _future.ToString();
    }
}
