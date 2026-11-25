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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;
using FutureTimeoutException = Confluent.Kafka.TimeoutException;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The engine tests run on the async engine, <see cref="AsyncKafkaFutureImpl{T}"/>, and the waits only it has: the
/// task of <c>ToCompletionStage</c> and the tasks of the <c>Get</c> forms.
/// </summary>
public sealed class AsyncKafkaFutureImplTests : KafkaFutureImplTestBase
{
    private static readonly FieldInfo s_stateField =
        typeof(AsyncKafkaFutureImpl<int>).GetField("_state", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static readonly FieldInfo s_taskField =
        typeof(FutureState<int>).GetField("_task", BindingFlags.NonPublic | BindingFlags.Instance)!;

    [Fact]
    public async Task ToCompletionStage_TheTaskIsMadeOnTheFirstAsyncWait_AndIsTheSameEveryTime()
    {
        var future = new AsyncKafkaFutureImpl<int>();
        _ = future.IsDone();
        _ = future.GetNowUnwrapped(0);
        _ = future.ToString();
        future.ThenApplyFunc(value => value);
        future.WhenCompleteAction((value, error) => { });
        Assert.Throws<FutureTimeoutException>(() => future.GetUnwrapped(TimeSpan.Zero));
        Assert.Null(s_taskField.GetValue(s_stateField.GetValue(future)));

        var task = future.ToCompletionStage();
        Assert.Same(task, ((TaskCompletionSource<int>)s_taskField.GetValue(s_stateField.GetValue(future))!).Task);
        Assert.Same(task, future.ToCompletionStage());
        Assert.Same(task, ((AsyncKafkaFuture)future).ToCompletionStage());
        Assert.Same(future.Get(), future.Get(Timeout.InfiniteTimeSpan));
        Assert.Same(future.Get(), future.Get(CancellationToken.None));
        Assert.False(task.IsCompleted);

        future.Complete(9);
        Assert.Same(task, future.ToCompletionStage());
        Assert.True(task.IsCompleted);
        Assert.Equal(9, await task);

        var completedFirst = FutureState.CompletedFuture(new AsyncKafkaFutureImpl<int>(), 10);
        Assert.Equal(10, Bounded(() => completedFirst.GetUnwrapped()));
        Assert.Null(s_taskField.GetValue(s_stateField.GetValue(completedFirst)));
        var lateTask = completedFirst.ToCompletionStage();
        Assert.True(lateTask.IsCompleted);
        Assert.Equal(10, await lateTask);
        Assert.Same(lateTask, completedFirst.ToCompletionStage());
    }

    [Fact]
    public async Task ToCompletionStage_ResumesItsAwaitersAsynchronously_NeverOnTheCompletingThread()
    {
        var future = new AsyncKafkaFutureImpl<int>();
        var resumedOn = future.ToCompletionStage().ContinueWith(
            _ => Environment.CurrentManagedThreadId,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        int completer = RunOnNewThread(() => future.Complete(1));

        Assert.Same(resumedOn, await Task.WhenAny(resumedOn, Task.Delay(s_deadline)));
        Assert.NotEqual(completer, await resumedOn);
    }

    [Fact]
    public async Task ToCompletionStage_AFailedFutureFaultsWithTheSameInstance_AndACancelledOneIsCanceled()
    {
        var error = new InvalidOperationException("failed");
        var failed = new AsyncKafkaFutureImpl<int>();
        var failedTask = failed.ToCompletionStage();
        failed.CompleteWithError(error);
        Assert.True(failedTask.IsFaulted);
        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => failedTask));

        var cancelled = new AsyncKafkaFutureImpl<int>();
        var cancelledTask = cancelled.ToCompletionStage();
        cancelled.Cancel(true);
        Assert.True(cancelledTask.IsCanceled);
        await Assert.ThrowsAsync<TaskCanceledException>(() => cancelledTask);

        var cancelledFirst = new AsyncKafkaFutureImpl<int>();
        cancelledFirst.CompleteWithError(new OperationCanceledException("cancelled first"));
        Assert.True(cancelledFirst.ToCompletionStage().IsCanceled);
    }

    [Fact]
    public void ToCompletionStage_ACancellationGivenAsTheErrorOfAFutureMadeUnableToCancel_FaultsIt()
    {
        var cancellation = new OperationCanceledException("Nuclear launch detected.");
        var future = new AsyncKafkaFutureImpl<int>(mayCancel: false);
        var task = future.ToCompletionStage();
        Assert.True(future.CompleteWithError(cancellation));

        Assert.True(task.IsFaulted);
        Assert.False(task.IsCanceled);
        Assert.Same(cancellation, task.Exception!.InnerException);

        var sourced = new FakeFutureValueSource<int>();
        sourced.Fail(cancellation);
        var fromSource = new AsyncKafkaFutureImpl<int>(mayCancel: false, source: sourced);
        Assert.True(fromSource.ToCompletionStage().IsFaulted);
    }

    [Fact]
    public async Task Get_ATimeout_FaultsWithTimeoutException_UnlessTheFutureCompletesFirst()
    {
        var future = new AsyncKafkaFutureImpl<int>();
        var zero = future.Get(TimeSpan.Zero);
        var negative = future.Get(TimeSpan.FromMilliseconds(-5));
        var clock = Stopwatch.StartNew();
        var timedOut = future.Get(TimeSpan.FromMilliseconds(50));
        var thrown = await Assert.ThrowsAsync<FutureTimeoutException>(() => WithinDeadline(timedOut));
        Assert.True(clock.Elapsed >= TimeSpan.FromMilliseconds(50), $"faulted after {clock.Elapsed}");
        Assert.Equal(string.Empty, thrown.Message);
        Assert.IsType<FutureTimeoutException>(zero.Exception!.InnerException);
        Assert.IsType<FutureTimeoutException>(negative.Exception!.InnerException);

        var waiting = future.Get(TimeSpan.FromMinutes(5));
        var longest = future.Get(TimeSpan.MaxValue);
        future.Complete(5);
        Assert.Equal(5, await WithinDeadline(waiting));
        Assert.Equal(5, await WithinDeadline(longest));
        Assert.Same(future.Get(), future.Get(TimeSpan.Zero));
    }

    [Fact]
    public async Task Get_ATokenThatFires_CancelsOnlyThatWait()
    {
        var future = new AsyncKafkaFutureImpl<int>();
        using var source = new CancellationTokenSource();
        var wait = future.Get(source.Token);
        Assert.False(wait.IsCompleted);

        source.Cancel();

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithinDeadline(wait));
        Assert.Equal(source.Token, thrown.CancellationToken);
        Assert.True(wait.IsCanceled);
        AssertPending(future);

        var already = future.Get(source.Token);
        Assert.True(already.IsCanceled);
        Assert.Equal(source.Token, (await Assert.ThrowsAnyAsync<OperationCanceledException>(() => already))
            .CancellationToken);

        future.Complete(2);
        Assert.Equal(2, await WithinDeadline(future.Get(source.Token)));
    }

    private protected override IEnumerable<(string Read, Func<IKafkaFutureImpl<int>, object?> Call)> FlavorWaits =>
        new (string, Func<IKafkaFutureImpl<int>, object?>)[]
        {
            ("ToCompletionStage", future => ((AsyncKafkaFuture<int>)future).ToCompletionStage().IsCompleted),
            ("Get", future => ((AsyncKafkaFuture<int>)future).Get().IsCompleted),
        };

    private protected override IKafkaFutureImpl<T> New<T>(bool mayCancel, IFutureValueSource<T>? source) =>
        new AsyncKafkaFutureImpl<T>(mayCancel, source);
}
