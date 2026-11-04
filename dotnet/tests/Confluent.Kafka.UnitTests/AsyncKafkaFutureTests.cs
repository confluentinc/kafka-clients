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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>KafkaFutureTest</c> run against <see cref="AsyncKafkaFuture{T}"/> and <see cref="AsyncKafkaFuture"/>,
/// plus the completion-stage cases only the async futures have.
/// </summary>
public sealed class AsyncKafkaFutureTests : KafkaFutureTestBase
{
    private protected override bool WaitsRethrowTheCancellation => false;

    [Fact]
    public async Task TestLeakCompletableFuture()
    {
        var impl = new KafkaFutureImpl<string>();
        var future = new AsyncKafkaFuture<string>(impl);
        Task<string> stage = future.ToCompletionStage();
        Assert.Same(future.Get(), stage);
        MethodInfo[] completers = stage.GetType()
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
            .Where(method => method.Name.StartsWith("Set", StringComparison.Ordinal)
                || method.Name.StartsWith("TrySet", StringComparison.Ordinal))
            .ToArray();
        Assert.Empty(completers);

        Task<string> derived = stage.ContinueWith(
            completed => completed.Result,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        Assert.False(future.IsDone);
        Assert.False(stage.IsCompleted);
        Assert.False(derived.IsCompleted);

        Assert.True(impl.Complete("done"));
        Assert.Equal("done", await Value(stage));
        Assert.Equal("done", await Value(derived));
    }

    [Fact]
    public async Task TestTheInnerFutureFollowsTheFuture()
    {
        var impl = new KafkaFutureImpl<int>();
        var future = new AsyncKafkaFuture<int>(impl);
        Task<int> inner = future.ToCompletionStage();
        Assert.Same(inner, future.ToCompletionStage());
        var completer = new Thread(() => impl.Complete(7));
        completer.IsBackground = true;
        completer.Start();
        Assert.Equal(7, await Value(inner));
        Assert.True(completer.Join(s_deadline));
        Assert.True(future.IsDone);
    }

    private protected override FutureView<T> Wrap<T>(KafkaFutureImpl<T> impl) =>
        new View<T>(new AsyncKafkaFuture<T>(impl));

    private protected override FutureView<T> CompletedFuture<T>(T value) =>
        new View<T>(AsyncKafkaFuture.CompletedFuture(value));

    private protected override FutureView AllOf(params FutureView<int>[] futures)
    {
        var erased = new AsyncKafkaFuture[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            erased[i] = ((View<int>)futures[i]).Future;
        }

        return new ErasedView(AsyncKafkaFuture.AllOf(erased));
    }

    /// <summary>An <see cref="AsyncKafkaFuture{T}"/> as the shared tests see it.</summary>
    private sealed class View<T> : FutureView<T>
    {
        private static readonly FieldInfo s_impl =
            typeof(AsyncKafkaFuture<T>).GetField("_impl", BindingFlags.NonPublic | BindingFlags.Instance)!;

        internal View(AsyncKafkaFuture<T> future) => Future = future;

        internal AsyncKafkaFuture<T> Future { get; }

        internal override KafkaFutureImpl<T> Impl => (KafkaFutureImpl<T>)s_impl.GetValue(Future)!;

        internal override bool IsDone => Future.IsDone;

        internal override bool IsCancelled => Future.IsCancelled;

        internal override bool IsCompletedExceptionally => Future.IsCompletedExceptionally;

        internal override bool Cancel(bool mayInterruptIfRunning) => Future.Cancel(mayInterruptIfRunning);

        internal override Task<T> Get() => Future.Get();

        internal override Task<T> Get(TimeSpan timeout) => Future.Get(timeout);

        internal override Task<T> Get(CancellationToken cancellationToken) => Future.Get(cancellationToken);

        internal override T GetNow(T valueIfAbsent) => Future.GetNow(valueIfAbsent);

        internal override FutureView<TR> ThenApply<TR>(Func<T, TR> function) =>
            new View<TR>(Future.ThenApply<TR>(value => function(value)));

        internal override FutureView<T> WhenComplete(Action<T?, Exception?> action) =>
            new View<T>(Future.WhenComplete((value, error) => action(value, error)));

        internal override string Describe() => Future.ToString();
    }

    /// <summary>An <see cref="AsyncKafkaFuture"/> as the shared tests see it.</summary>
    private sealed class ErasedView : FutureView
    {
        private readonly AsyncKafkaFuture _future;

        internal ErasedView(AsyncKafkaFuture future) => _future = future;

        internal override bool IsDone => _future.IsDone;

        internal override bool IsCancelled => _future.IsCancelled;

        internal override bool IsCompletedExceptionally => _future.IsCompletedExceptionally;

        internal override bool Cancel(bool mayInterruptIfRunning) => _future.Cancel(mayInterruptIfRunning);

        internal override Task Wait() => _future.Get();

        internal override Task Wait(TimeSpan timeout) => _future.Get(timeout);

        internal override void WaitNow() => _future.GetNow();

        internal override string Describe() => _future.ToString();
    }
}
