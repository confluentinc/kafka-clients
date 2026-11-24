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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;
using FutureTimeoutException = Confluent.Kafka.TimeoutException;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests a future engine, the logic of <see cref="FutureState{T}"/> the two flavors share: completion, cancellation,
/// the waits, dependants, <c>WhenComplete</c> actions, <c>AllOf</c> and value sources. Each derived class runs them on
/// its flavor's engine.
/// </summary>
public abstract class KafkaFutureImplTestBase
{
    private protected static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Complete_TheFirstCompletionWins_AndEveryReadAnswersItsValue()
    {
        var future = New<int>();
        AssertPending(future);
        Assert.Equal(-1, future.GetNowUnwrapped(-1));

        Assert.True(future.Complete(12));
        Assert.False(future.Complete(13));
        Assert.False(future.CompleteWithError(new InvalidOperationException("late")));
        Assert.False(future.Cancel(true));

        Assert.True(future.IsDone());
        Assert.False(future.IsCompletedExceptionally());
        Assert.False(future.IsCancelled());
        Assert.Null(future.Failure);
        Assert.Equal(12, Bounded(() => future.GetUnwrapped()));
        Assert.Equal(12, future.GetUnwrapped(TimeSpan.Zero));
        Assert.Equal(12, Bounded(() => future.GetUnwrapped(new CancellationTokenSource().Token)));
        Assert.Equal(12, future.GetNowUnwrapped(-1));
    }

    [Fact]
    public void CompleteWithError_EveryWaitRethrowsTheSameInstance()
    {
        var error = new InvalidOperationException("Ultimate efficiency achieved.");
        var future = New<string>();
        Assert.True(future.CompleteWithError(error));
        Assert.False(future.Complete("late"));
        Assert.False(future.CompleteWithError(new InvalidOperationException("later")));

        Assert.True(future.IsDone());
        Assert.True(future.IsCompletedExceptionally());
        Assert.False(future.IsCancelled());
        Assert.Same(error, future.Failure);
        AssertThrowsSame(error, () => future.GetUnwrapped());
        AssertThrowsSame(error, () => future.GetUnwrapped(TimeSpan.FromMinutes(5)));
        AssertThrowsSame(error, () => future.GetUnwrapped(new CancellationTokenSource().Token));
        AssertThrowsSame(error, () => future.GetNowUnwrapped("I am ready"));
        AssertThrowsSame(error, () => future.GetNowUnwrapped(null!));
        Assert.Equal("Ultimate efficiency achieved.", error.Message);
    }

    [Fact]
    public void CompleteWithError_AnOperationCanceledException_CancelsWithThatInstance()
    {
        var cancellation = new OperationCanceledException("We require more vespene gas");
        var future = New<int>();
        Assert.True(future.CompleteWithError(cancellation));

        Assert.True(future.IsDone());
        Assert.True(future.IsCancelled());
        Assert.True(future.IsCompletedExceptionally());
        AssertThrowsSame(cancellation, () => future.GetUnwrapped());
        AssertThrowsSame(cancellation, () => future.GetUnwrapped(TimeSpan.Zero));
        AssertThrowsSame(cancellation, () => future.GetNowUnwrapped(0));
        Assert.Equal("We require more vespene gas", cancellation.Message);
    }

    [Fact]
    public void Cancel_APendingFuture_CancelsIt_AndAnswersTrueAgain_AsJavaDoes()
    {
        var future = New<int>();
        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        Assert.False(future.Complete(1));
        Assert.False(future.CompleteWithError(new InvalidOperationException("late")));

        Assert.True(future.IsDone());
        Assert.True(future.IsCancelled());
        Assert.True(future.IsCompletedExceptionally());
        var thrown = Assert.Throws<OperationCanceledException>(() => Bounded(() => future.GetUnwrapped()));
        Assert.Equal(new OperationCanceledException().Message, thrown.Message);
        Assert.Same(thrown, future.Failure);
        AssertThrowsSame(thrown, () => future.GetUnwrapped(TimeSpan.Zero));
        AssertThrowsSame(thrown, () => future.GetNowUnwrapped(0));
    }

    [Fact]
    public void Cancel_ADoneFuture_AnswersFalse_AndChangesNothing()
    {
        var succeeded = FutureState.CompletedFuture(New<int>(), 5);
        Assert.False(succeeded.Cancel(true));
        Assert.False(succeeded.IsCancelled());
        Assert.Equal(5, Bounded(() => succeeded.GetUnwrapped()));

        var error = new InvalidOperationException("failed");
        var failed = New<int>();
        failed.CompleteWithError(error);
        Assert.False(failed.Cancel(true));
        Assert.False(failed.IsCancelled());
        Assert.Same(error, failed.Failure);
    }

    [Fact]
    public void Cancel_OnAFutureMadeUnableToCancel_AnswersFalse_AndChangesNothing_WhileItsDependantsStillCancel()
    {
        var future = New<int>(mayCancel: false);
        var dependant = future.ThenApplyFunc(value => value * 2);

        Assert.False(future.Cancel(true));
        Assert.False(future.Cancel(false));
        AssertPending(future);

        Assert.True(dependant.Cancel(true));
        Assert.True(dependant.IsCancelled());
        AssertPending(future);

        Assert.True(future.Complete(21));
        Assert.False(future.Cancel(true));
        Assert.False(future.IsCancelled());
        Assert.Equal(21, Bounded(() => future.GetUnwrapped()));
        Assert.Throws<OperationCanceledException>(() => Bounded(() => dependant.GetUnwrapped()));
    }

    [Fact]
    public void CompleteWithError_AnOperationCanceledException_OnAFutureMadeUnableToCancel_IsAnOrdinaryFailure()
    {
        var cancellation = new OperationCanceledException("Nuclear launch detected.");
        var future = New<int>(mayCancel: false);
        Assert.True(future.CompleteWithError(cancellation));

        Assert.True(future.IsDone());
        Assert.False(future.IsCancelled());
        Assert.True(future.IsCompletedExceptionally());
        Assert.False(future.Cancel(true));
        Assert.False(future.IsCancelled());
        Assert.Same(cancellation, future.Failure);
        AssertThrowsSame(cancellation, () => future.GetUnwrapped());
        AssertThrowsSame(cancellation, () => future.GetUnwrapped(TimeSpan.Zero));
        AssertThrowsSame(cancellation, () => future.GetNowUnwrapped(0));

        Assert.Equal(
            "KafkaFuture{value=null,exception=System.OperationCanceledException: Nuclear launch detected.,done=true}",
            future.ToString());

        var sourced = new FakeFutureValueSource<int>();
        sourced.Fail(cancellation);
        var fromSource = New<int>(mayCancel: false, source: sourced);
        Assert.True(fromSource.IsCompletedExceptionally());
        Assert.False(fromSource.IsCancelled());
    }

    [Fact]
    public void Cancel_AThenApplyDependantCancelledThroughItsSource_AnswersFalse_ThoughItIsCancelled()
    {
        var source = New<int>();
        var registeredBefore = source.ThenApplyFunc(value => value);
        Assert.True(source.Cancel(true));
        var registeredAfter = source.ThenApplyFunc(value => value);

        foreach (var dependant in new[] { registeredBefore, registeredAfter })
        {
            Assert.True(dependant.IsCancelled());
            Assert.False(dependant.Cancel(true));
            Assert.False(dependant.Cancel(false));
            Assert.True(dependant.IsCancelled());
            Assert.Same(source.Failure, dependant.Failure);
        }

        Assert.True(source.Cancel(true));
    }

    [Fact]
    public void Cancel_AThenApplyDependantCancelledByItsFunction_AnswersFalse_ThoughItIsCancelled()
    {
        var cancellation = new OperationCanceledException("function cancelled");
        var dependant = FutureState.CompletedFuture(New<int>(), 1).ThenApplyFunc<int>(_ => throw cancellation);

        Assert.True(dependant.IsCancelled());
        Assert.False(dependant.Cancel(true));
        Assert.True(dependant.IsCancelled());
        Assert.Same(cancellation, dependant.Failure);
    }

    [Fact]
    public void Cancel_AWhenCompleteDependantCancelledThroughItsSourceOrItsAction_AnswersFalse_ThoughItIsCancelled()
    {
        var source = New<int>();
        var registeredBefore = source.WhenCompleteAction((value, error) => { });
        Assert.True(source.Cancel(true));
        var registeredAfter = source.WhenCompleteAction((value, error) => { });
        var cancellation = new OperationCanceledException("action cancelled");
        var byItsAction = FutureState.CompletedFuture(New<int>(), 2).WhenCompleteAction((value, error) => throw cancellation);

        foreach (var dependant in new[] { registeredBefore, registeredAfter, byItsAction })
        {
            Assert.True(dependant.IsCancelled());
            Assert.False(dependant.Cancel(true));
            Assert.True(dependant.IsCancelled());
        }

        Assert.Same(cancellation, byItsAction.Failure);
    }

    [Fact]
    public void Cancel_AFutureCancelledByItsOwnCompletion_OrAnAllOfResultOfACancelledFuture_AnswersTrue()
    {
        var completedCancelled = New<int>();
        Assert.True(completedCancelled.CompleteWithError(new OperationCanceledException("cancelled")));
        Assert.True(completedCancelled.Cancel(true));

        var cancelled = New<int>();
        var all = FutureState.AllOf(New<object?>(), new IKafkaFutureImpl[] { cancelled });
        cancelled.Cancel(true);
        Assert.True(all.IsCancelled());
        Assert.True(all.Cancel(true));
    }

    [Fact]
    public void Get_WaitsForACompletionFromAnotherThread_AndWakesEveryWaiter()
    {
        var future = New<string>();
        PreJit(future);
        var first = new Waiter<string>(() => future.GetUnwrapped());
        var second = new Waiter<string>(() => future.GetUnwrapped(TimeSpan.FromMinutes(5)));
        var third = new Waiter<string>(() => future.GetUnwrapped(new CancellationTokenSource().Token));
        Assert.True(first.IsBlocked());
        Assert.True(second.IsBlocked());
        Assert.True(third.IsBlocked());

        RunOnNewThread(() => future.Complete("You must construct additional pylons."));

        foreach (var waiter in new[] { first, second, third })
        {
            Assert.True(waiter.Join());
            Assert.Null(waiter.Error);
            Assert.Equal("You must construct additional pylons.", waiter.Result);
        }
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(-20_000L)]
    [InlineData(long.MinValue)]
    public void Get_AZeroOrNegativeTimeout_ChecksOnce_AndThrowsTimeoutExceptionUnlessDone(long ticks)
    {
        var timeout = TimeSpan.FromTicks(ticks);
        var future = New<int>();
        var thrown = Assert.Throws<FutureTimeoutException>(() => future.GetUnwrapped(timeout));
        Assert.Equal(string.Empty, thrown.Message);
        AssertPending(future);

        future.Complete(3);
        Assert.Equal(3, future.GetUnwrapped(timeout));
    }

    [Fact]
    public void Get_APositiveTimeout_ThrowsTimeoutExceptionOnceItPasses()
    {
        var timeout = TimeSpan.FromMilliseconds(50);
        var future = New<int>();
        var clock = Stopwatch.StartNew();
        var thrown = Assert.Throws<FutureTimeoutException>(() => future.GetUnwrapped(timeout));
        Assert.True(clock.Elapsed >= timeout, $"returned after {clock.Elapsed}");
        Assert.Equal(string.Empty, thrown.Message);
        AssertPending(future);
    }

    [Fact]
    public void Get_InfiniteTimeSpan_AndATimeoutPastIntMaxValueMilliseconds_WaitLikeGet()
    {
        var future = New<int>();
        PreJit(future);
        var infinite = new Waiter<int>(() => future.GetUnwrapped(Timeout.InfiniteTimeSpan));
        var pastIntMax = new Waiter<int>(() => future.GetUnwrapped(TimeSpan.FromMilliseconds(int.MaxValue + 1.0)));
        var longest = new Waiter<int>(() => future.GetUnwrapped(TimeSpan.MaxValue));
        Assert.True(infinite.IsBlocked());
        Assert.True(pastIntMax.IsBlocked());
        Assert.True(longest.IsBlocked());

        future.Complete(8);

        foreach (var waiter in new[] { infinite, pastIntMax, longest })
        {
            Assert.True(waiter.Join());
            Assert.Null(waiter.Error);
            Assert.Equal(8, waiter.Result);
        }
    }

    [Fact]
    public void Get_ATokenThatFiresWhileWaiting_ThrowsOperationCanceledExceptionForIt_AndTheFutureIsUnchanged()
    {
        var future = New<int>();
        PreJit(future);
        using var source = new CancellationTokenSource();
        var waiter = new Waiter<int>(() => future.GetUnwrapped(source.Token));
        Assert.True(waiter.IsBlocked());

        source.Cancel();

        Assert.True(waiter.Join());
        var thrown = Assert.IsType<OperationCanceledException>(waiter.Error);
        Assert.Equal(source.Token, thrown.CancellationToken);
        Assert.Equal(new OperationCanceledException().Message, thrown.Message);
        AssertPending(future);
        Assert.True(future.Complete(4));
        Assert.Equal(4, Bounded(() => future.GetUnwrapped()));
    }

    [Fact]
    public void Get_ATokenAlreadyCancelled_ThrowsOnAPendingFuture_AndAnswersADoneOne()
    {
        using var source = new CancellationTokenSource();
        source.Cancel();
        var future = New<int>();
        var thrown = Assert.Throws<OperationCanceledException>(() => Bounded(() => future.GetUnwrapped(source.Token)));
        Assert.Equal(source.Token, thrown.CancellationToken);
        AssertPending(future);

        future.Complete(6);
        Assert.Equal(6, Bounded(() => future.GetUnwrapped(source.Token)));
    }

    [Fact]
    public void ThenApply_RunsTheFunctionOnTheCompletingThread_BeforeCompleteReturns()
    {
        var future = New<int>();
        int functionThread = 0;
        var dependant = future.ThenApplyFunc(value =>
        {
            functionThread = Environment.CurrentManagedThreadId;
            return value + 1;
        });
        AssertPending(dependant);

        int completer = RunOnNewThread(() => future.Complete(1));

        Assert.Equal(completer, functionThread);
        Assert.True(dependant.IsDone());
        Assert.Equal(2, dependant.GetNowUnwrapped(-1));
    }

    [Fact]
    public void ThenApply_OnADoneFuture_RunsTheFunctionInsideTheRegisteringCall()
    {
        var future = FutureState.CompletedFuture(New<int>(), 1);
        int functionThread = 0;
        var dependant = future.ThenApplyFunc(value =>
        {
            functionThread = Environment.CurrentManagedThreadId;
            return value + 1;
        });

        Assert.Equal(Environment.CurrentManagedThreadId, functionThread);
        Assert.True(dependant.IsDone());
        Assert.Equal(2, dependant.GetNowUnwrapped(-1));
    }

    [Fact]
    public void ThenApply_CarriesAFailureOrACancellationUnchanged_WithoutCallingTheFunction()
    {
        var error = new InvalidOperationException("source failed");
        var failed = New<int>();
        var cancelled = New<int>();
        int calls = 0;
        var fromFailed = failed.ThenApplyFunc(value => ++calls);
        var fromCancelled = cancelled.ThenApplyFunc(value => ++calls);

        failed.CompleteWithError(error);
        cancelled.Cancel(true);

        Assert.Equal(0, calls);
        Assert.True(fromFailed.IsCompletedExceptionally());
        Assert.False(fromFailed.IsCancelled());
        AssertThrowsSame(error, () => fromFailed.GetUnwrapped());
        Assert.True(fromCancelled.IsCancelled());
        Assert.Same(cancelled.Failure, fromCancelled.Failure);
    }

    [Fact]
    public void ThenApply_AFunctionThatThrows_FailsTheDependantWithIt_AndAnOperationCanceledExceptionCancelsIt()
    {
        var error = new ArgumentException("function failed");
        var cancellation = new OperationCanceledException("function cancelled");
        var future = FutureState.CompletedFuture(New<int>(), 1);

        var failing = future.ThenApplyFunc<int>(_ => throw error);
        var cancelling = future.ThenApplyFunc<int>(_ => throw cancellation);

        Assert.True(failing.IsCompletedExceptionally());
        Assert.False(failing.IsCancelled());
        AssertThrowsSame(error, () => failing.GetUnwrapped());
        Assert.True(cancelling.IsCancelled());
        AssertThrowsSame(cancellation, () => cancelling.GetUnwrapped());
        Assert.Equal(1, Bounded(() => future.GetUnwrapped()));
    }

    [Fact]
    public void ThenApply_WithoutAnArgument_RunsOnSuccess()
    {
        IKafkaFutureImpl future = New<object?>();
        var dependant = future.ThenApply(() => "applied");
        AssertPending(dependant);

        ((IKafkaFutureImpl<object?>)future).Complete(null);

        Assert.Equal("applied", dependant.GetNowUnwrapped("absent"));
    }

    [Fact]
    public void WhenComplete_RunsTheActionOnTheCompletingThread_WithTheValueOrTheFailure()
    {
        var succeeded = New<int>();
        var failed = New<int>();
        var error = new InvalidOperationException("source failed");
        var seen = new List<(int Value, Exception? Error, int Thread)>();
        var fromSucceeded = succeeded.WhenCompleteAction((value, failure) =>
            seen.Add((value, failure, Environment.CurrentManagedThreadId)));
        var fromFailed = failed.WhenCompleteAction((value, failure) =>
            seen.Add((value, failure, Environment.CurrentManagedThreadId)));

        int firstCompleter = RunOnNewThread(() => succeeded.Complete(7));
        int secondCompleter = RunOnNewThread(() => failed.CompleteWithError(error));

        Assert.Equal(
            new List<(int, Exception?, int)> { (7, null, firstCompleter), (0, error, secondCompleter) },
            seen);
        Assert.Equal(7, fromSucceeded.GetNowUnwrapped(-1));
        AssertThrowsSame(error, () => fromFailed.GetNowUnwrapped(-1));
    }

    [Fact]
    public void WhenComplete_OnADoneFuture_RunsTheActionInsideTheRegisteringCall()
    {
        var future = FutureState.CompletedFuture(New<string>(), "done");
        int actionThread = 0;
        var dependant = future.WhenCompleteAction((value, error) => actionThread = Environment.CurrentManagedThreadId);

        Assert.Equal(Environment.CurrentManagedThreadId, actionThread);
        Assert.Equal("done", dependant.GetNowUnwrapped("absent"));
    }

    [Fact]
    public void WhenComplete_AnActionThatThrows_FailsTheDependantWithIt_OnlyWhenTheFutureSucceeded()
    {
        var actionError = new ArgumentException("action failed");
        var sourceError = new InvalidOperationException("source failed");
        var failed = New<int>();
        failed.CompleteWithError(sourceError);

        var fromSucceeded = FutureState.CompletedFuture(New<int>(), 1).WhenCompleteAction((value, error) => throw actionError);
        var fromFailed = failed.WhenCompleteAction((value, error) => throw actionError);

        AssertThrowsSame(actionError, () => fromSucceeded.GetUnwrapped());
        AssertThrowsSame(sourceError, () => fromFailed.GetUnwrapped());
    }

    [Fact]
    public void WhenComplete_OnACancelledFuture_SeesTheCancellation_AndTheDependantIsCancelled()
    {
        var future = New<int>();
        Exception? seen = null;
        var dependant = future.WhenCompleteAction((value, error) => seen = error);
        future.Cancel(true);

        Assert.IsType<OperationCanceledException>(seen);
        Assert.Same(future.Failure, seen);
        Assert.True(dependant.IsCancelled());
    }

    [Fact]
    public void WhenComplete_WithoutAValue_SeesOnlyTheFailure()
    {
        var error = new InvalidOperationException("source failed");
        var source = New<object?>();
        IKafkaFutureImpl future = source;
        var seen = new List<Exception?>();
        var dependant = future.WhenComplete(seen.Add);

        source.CompleteWithError(error);

        Assert.Equal(new List<Exception?> { error }, seen);
        Assert.True(dependant.IsCompletedExceptionally());
        AssertThrowsSame(error, () => ((IKafkaFutureImpl<object?>)dependant).GetNowUnwrapped(null));
    }

    [Fact]
    public void Listeners_RunOutsideTheFuturesLock_SoAnotherThreadCanReadTheFutureWhileTheyRun()
    {
        var error = new InvalidOperationException("failed");
        var completed = New<int>();
        var failed = New<int>();
        var cancelled = New<int>();
        var reads = new ConcurrentQueue<(string Path, bool Returned, Exception? Error)>();
        void Read(string path, IKafkaFutureImpl<int> future)
        {
            var (returned, readError) = ReadOnAnotherThread(future);
            reads.Enqueue((path, returned, readError));
        }

        completed.WhenCompleteAction((value, e) => Read("Complete", completed));
        failed.WhenCompleteAction((value, e) => Read("CompleteWithError", failed));
        cancelled.WhenCompleteAction((value, e) => Read("Cancel", cancelled));
        completed.Complete(1);
        failed.CompleteWithError(error);
        cancelled.Cancel(true);
        completed.ThenApplyFunc(value =>
        {
            Read("registered on a done future", completed);
            return value;
        });

        var seen = reads.ToArray();
        Assert.Equal(
            new[] { "Complete", "CompleteWithError", "Cancel", "registered on a done future" },
            Array.ConvertAll(seen, read => read.Path));
        foreach (var (path, returned, _) in seen)
        {
            Assert.True(returned, path + ": a read from another thread waited for the listener to return");
        }

        Assert.Null(seen[0].Error);
        Assert.Same(error, seen[1].Error);
        Assert.Same(cancelled.Failure, seen[2].Error);
        Assert.Null(seen[3].Error);
    }

    [Fact]
    public void AllOf_NoFutures_IsAlreadyDone()
    {
        var all = FutureState.AllOf(New<object?>(), Array.Empty<IKafkaFutureImpl>());

        Assert.True(all.IsDone());
        Assert.False(all.IsCompletedExceptionally());
        Assert.Null(all.GetUnwrapped(TimeSpan.Zero));
    }

    [Fact]
    public void AllOf_CompletesWhenTheLastFutureCompletes_OnThatFuturesCompletingThread()
    {
        var first = New<int>();
        var second = New<string>();
        var all = FutureState.AllOf(New<object?>(), new IKafkaFutureImpl[] { first, second, first });
        int actionThread = 0;
        all.WhenCompleteAction((value, error) => actionThread = Environment.CurrentManagedThreadId);

        first.Complete(1);
        AssertPending(all);

        int completer = RunOnNewThread(() => second.Complete("two"));

        Assert.Equal(completer, actionThread);
        Assert.True(all.IsDone());
        Assert.False(all.IsCompletedExceptionally());
        Assert.Null(all.GetNowUnwrapped("absent"));
    }

    [Fact]
    public void AllOf_FailsWithTheFirstFailureToArrive_OnceEveryFutureIsDone()
    {
        var first = New<int>();
        var second = New<int>();
        var third = New<int>();
        var all = FutureState.AllOf(New<object?>(), new IKafkaFutureImpl[] { first, second, third });
        var earlier = new InvalidOperationException("third failed first");
        var later = new ArgumentException("first failed later");

        third.CompleteWithError(earlier);
        first.CompleteWithError(later);
        AssertPending(all);
        second.Complete(2);

        Assert.True(all.IsCompletedExceptionally());
        Assert.False(all.IsCancelled());
        AssertThrowsSame(earlier, () => all.GetUnwrapped());
    }

    [Fact]
    public void AllOf_ACancelledFuture_CancelsTheResult()
    {
        var first = New<int>();
        var second = New<int>();
        var all = FutureState.AllOf(New<object?>(), new IKafkaFutureImpl[] { first, second });

        second.Cancel(true);
        first.Complete(1);

        Assert.True(all.IsCancelled());
        Assert.Same(second.Failure, all.Failure);
    }

    [Fact]
    public void ToString_UsesJavasFormat()
    {
        var success = New<int>();
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", success.ToString());
        success.Complete(12);
        Assert.Equal("KafkaFuture{value=12,exception=null,done=true}", success.ToString());

        var failure = New<int>();
        failure.CompleteWithError(new InvalidOperationException("foo"));
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.InvalidOperationException: foo,done=true}", failure.ToString());

        var cancelled = New<int>();
        cancelled.Cancel(true);
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.OperationCanceledException: "
                + new OperationCanceledException().Message + ",done=true}",
            cancelled.ToString());

        var noValue = New<object?>();
        noValue.Complete(null);
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", noValue.ToString());
    }

    [Fact]
    public void CompleteAndCompleteWithError_FromManyThreadsAtOnce_LetExactlyOneWin()
    {
        const int Threads = 8;
        for (int round = 0; round < 50; round++)
        {
            var future = New<int>();
            var winners = new ConcurrentQueue<int>();
            using var start = new Barrier(Threads);
            var threads = new Thread[Threads];
            for (int i = 0; i < Threads; i++)
            {
                int index = i;
                threads[i] = new Thread(() =>
                {
                    start.SignalAndWait();
                    bool won = index % 2 == 0
                        ? future.Complete(index)
                        : future.CompleteWithError(new InvalidOperationException("thread " + index));
                    if (won)
                    {
                        winners.Enqueue(index);
                    }
                });
                threads[i].IsBackground = true;
                threads[i].Start();
            }

            foreach (var thread in threads)
            {
                Assert.True(thread.Join(s_deadline));
            }

            int winner = Assert.Single(winners);
            if (winner % 2 == 0)
            {
                Assert.Equal(winner, Bounded(() => future.GetUnwrapped()));
            }
            else
            {
                var thrown = Assert.Throws<InvalidOperationException>(() => Bounded(() => future.GetUnwrapped()));
                Assert.Equal("thread " + winner, thrown.Message);
            }
        }
    }

    [Fact]
    public void ADoneValueSource_SettlesTheFutureOnTheReadingThread_AndIsNotReadAgain()
    {
        var source = new FakeFutureValueSource<int>();
        var future = New<int>(source: source);
        int actionThread = 0;
        future.WhenCompleteAction((value, error) => actionThread = Environment.CurrentManagedThreadId);
        Assert.Equal(-1, future.GetNowUnwrapped(-1));
        Assert.Equal(1, source.Reads);

        source.Finish(42);
        int reader = RunOnNewThread(() => Assert.Equal(42, future.GetNowUnwrapped(-1)));

        Assert.Equal(reader, actionThread);
        Assert.Equal(2, source.Reads);
        Assert.Equal(42, Bounded(() => future.GetUnwrapped()));
        Assert.Equal(42, future.GetNowUnwrapped(-1));
        Assert.False(future.Cancel(true));
        Assert.False(future.Complete(43));
        Assert.Equal(2, source.Reads);
    }

    [Fact]
    public void ADoneValueSource_IsReadByEveryStateReadButIsDone_AndByEveryWait()
    {
        var reads = new List<(string Read, Func<IKafkaFutureImpl<int>, object?> Call)>
        {
            ("IsCancelled", future => future.IsCancelled()),
            ("IsCompletedExceptionally", future => future.IsCompletedExceptionally()),
            ("GetNowUnwrapped", future => future.GetNowUnwrapped(-1)),
            ("ToString", future => future.ToString()),
            ("GetUnwrapped", future => future.GetUnwrapped()),
            ("GetUnwrapped(TimeSpan)", future => future.GetUnwrapped(TimeSpan.Zero)),
            ("GetUnwrapped(CancellationToken)", future => future.GetUnwrapped(new CancellationTokenSource().Token)),
        };
        reads.AddRange(FlavorWaits);
        foreach (var (read, call) in reads)
        {
            var source = new FakeFutureValueSource<int>();
            source.Finish(7);
            var future = New<int>(source: source);

            _ = Bounded(() => call(future));

            Assert.True(1 == source.Reads, read);
            Assert.True(future.Complete(8) == false, read);
            Assert.True(future.GetNowUnwrapped(-1) == 7, read);
        }
    }

    [Fact]
    public void IsDone_OnADoneValueSource_OnlyAsksTheSource_AndSettlesNothing()
    {
        var source = new FakeFutureValueSource<int>();
        var future = New<int>(source: source);
        int ran = 0;
        future.WhenCompleteAction((value, error) => Interlocked.Increment(ref ran));
        Assert.False(future.IsDone());

        source.Finish(7);
        Assert.True(future.IsDone());
        Assert.True(future.IsDone());
        future.WhenCompleteAction((value, error) => Interlocked.Increment(ref ran));

        Assert.Equal(0, Volatile.Read(ref ran));
        Assert.Equal(0, source.Reads);
        Assert.Equal(7, future.GetNowUnwrapped(-1));
        Assert.Equal(2, Volatile.Read(ref ran));
        Assert.True(future.IsDone());
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public void Cancel_OnAPendingFutureWhoseSourceIsDone_AnswersFalse_AndReadsAndRunsNothing_UntilASettlingRead()
    {
        var source = new FakeFutureValueSource<int>();
        var future = New<int>(source: source);
        int applied = 0;
        int actions = 0;
        var dependant = future.ThenApplyFunc(value =>
        {
            Interlocked.Increment(ref applied);
            return value + 1;
        });
        future.WhenCompleteAction((value, error) => Interlocked.Increment(ref actions));
        source.Finish(7);

        Assert.False(future.Cancel(true));
        Assert.False(future.Cancel(false));

        Assert.Equal(0, source.Reads);
        Assert.Equal(0, Volatile.Read(ref applied));
        Assert.Equal(0, Volatile.Read(ref actions));
        Assert.False(dependant.IsDone());
        Assert.True(future.IsDone());

        Assert.Equal(7, future.GetNowUnwrapped(-1));
        Assert.Equal(1, source.Reads);
        Assert.Equal(1, Volatile.Read(ref applied));
        Assert.Equal(1, Volatile.Read(ref actions));
        Assert.Equal(8, dependant.GetNowUnwrapped(-1));
        Assert.False(future.IsCancelled());
        Assert.False(future.Cancel(true));
        Assert.Equal(7, Bounded(() => future.GetUnwrapped()));
        Assert.Equal(1, source.Reads);
        Assert.Equal(1, Volatile.Read(ref applied));
        Assert.Equal(1, Volatile.Read(ref actions));
    }

    [Fact]
    public void Cancel_OnAPendingFutureWhoseSourceWasCancelled_AnswersFalseUntilASettlingRead_AndTrueAfterIt()
    {
        var cancellation = new OperationCanceledException("native cancellation");
        var source = new FakeFutureValueSource<int>();
        source.Fail(cancellation);
        var future = New<int>(source: source);

        Assert.False(future.Cancel(true));
        Assert.False(future.Cancel(false));
        Assert.Equal(0, source.Reads);

        Assert.True(future.IsCancelled());
        Assert.Equal(1, source.Reads);
        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        AssertThrowsSame(cancellation, () => future.GetNowUnwrapped(0));
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public void Cancel_OnAFutureCancelledBeforeItsSourceIsDone_AnswersTrueAgain_AndReadsNothing()
    {
        var source = new FakeFutureValueSource<int>();
        var future = New<int>(source: source);

        Assert.True(future.Cancel(true));
        source.Finish(7);

        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        Assert.True(future.IsCancelled());
        var thrown = Assert.Throws<OperationCanceledException>(() => future.GetNowUnwrapped(-1));
        Assert.Equal(new OperationCanceledException().Message, thrown.Message);
        Assert.Equal(0, source.Reads);
    }

    [Fact]
    public void AFailedValueSource_FailsTheFutureWithItsError_AndACancellationCancelsIt()
    {
        var error = new InvalidOperationException("native failure");
        var failedSource = new FakeFutureValueSource<int>();
        failedSource.Fail(error);
        var failed = New<int>(source: failedSource);
        Assert.True(failed.IsCompletedExceptionally());
        AssertThrowsSame(error, () => failed.GetUnwrapped());

        var cancellation = new OperationCanceledException("native cancellation");
        var cancelledSource = new FakeFutureValueSource<int>();
        cancelledSource.Fail(cancellation);
        var cancelled = New<int>(source: cancelledSource);
        Assert.True(cancelled.IsCancelled());
        AssertThrowsSame(cancellation, () => cancelled.GetNowUnwrapped(0));
    }

    [Fact]
    public void Get_OnAFutureWithAValueSource_AnswersOnceTheSourceIsDone_ThoughNothingCompletesTheFuture()
    {
        var source = new FakeFutureValueSource<string>();
        var future = New<string>(source: source);
        PreJit(future);
        var untimed = new Waiter<string>(() => future.GetUnwrapped());
        var timed = new Waiter<string>(() => future.GetUnwrapped(TimeSpan.MaxValue));
        Assert.True(untimed.IsBlocked());
        Assert.True(timed.IsBlocked());

        source.Finish("from the handle");

        foreach (var waiter in new[] { untimed, timed })
        {
            Assert.True(waiter.Join());
            Assert.Null(waiter.Error);
            Assert.Equal("from the handle", waiter.Result);
        }
    }

    private protected abstract IKafkaFutureImpl<T> New<T>(bool mayCancel = true, IFutureValueSource<T>? source = null);

    // The waits only this flavor's engine has, each read once from a future whose value source is done.
    private protected virtual IEnumerable<(string Read, Func<IKafkaFutureImpl<int>, object?> Call)> FlavorWaits =>
        Array.Empty<(string, Func<IKafkaFutureImpl<int>, object?>)>();

    private protected static void AssertPending(IKafkaFutureImpl future)
    {
        Assert.False(future.IsDone());
        Assert.False(future.IsCompletedExceptionally());
        Assert.False(future.IsCancelled());
    }

    private static void AssertThrowsSame(Exception expected, Action action)
    {
        var thrown = Assert.Throws(expected.GetType(), () => { _ = Bounded(() => { action(); return 0; }); });
        Assert.Same(expected, thrown);
    }

    private static void AssertThrowsSame<TResult>(Exception expected, Func<TResult> function) =>
        AssertThrowsSame(expected, () => { _ = function(); });

    private protected static TResult Bounded<TResult>(Func<TResult> wait)
    {
        TResult result = default!;
        ExceptionDispatchInfo? error = null;
        var thread = new Thread(() =>
        {
            try
            {
                result = wait();
            }
            catch (Exception e)
            {
                error = ExceptionDispatchInfo.Capture(e);
            }
        });
        thread.IsBackground = true;
        thread.Start();
        Assert.True(thread.Join(s_deadline), "the wait did not return within the deadline");
        error?.Throw();
        return result;
    }

    private protected static async Task<TResult> WithinDeadline<TResult>(Task<TResult> task)
    {
        Assert.True(
            ReferenceEquals(task, await Task.WhenAny(task, Task.Delay(s_deadline))),
            "the task did not complete within the deadline");
        return await task;
    }

    private static (bool Returned, Exception? Error) ReadOnAnotherThread(IKafkaFutureImpl<int> future)
    {
        Exception? error = null;
        var reader = new Thread(() =>
        {
            try
            {
                _ = future.GetUnwrapped(TimeSpan.Zero);
            }
            catch (Exception e)
            {
                error = e;
            }
        });
        reader.IsBackground = true;
        reader.Start();
        bool returned = reader.Join(s_deadline);
        return (returned, error);
    }

    private void PreJit<TValue>(IKafkaFutureImpl<TValue> pending)
    {
        var done = New<TValue>();
        done.Complete(default!);
        _ = Bounded(() => done.GetUnwrapped());
        _ = Bounded(() => done.GetUnwrapped(TimeSpan.MaxValue));
        _ = Bounded(() => done.GetUnwrapped(new CancellationTokenSource().Token));
        Assert.False(pending.IsDone());
    }

    private protected static int RunOnNewThread(Action action)
    {
        int threadId = 0;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            threadId = Environment.CurrentManagedThreadId;
            try
            {
                action();
            }
            catch (Exception e)
            {
                error = e;
            }
        });
        thread.IsBackground = true;
        thread.Start();
        Assert.True(thread.Join(s_deadline));
        Assert.Null(error);
        return threadId;
    }

    /// <summary>
    /// A wait run on its own background thread, which reports whether that thread is parked in it and keeps what it
    /// returned or threw.
    /// </summary>
    private sealed class Waiter<TResult>
    {
        private readonly Thread _thread;

        private volatile bool _calling;

        internal Waiter(Func<TResult> wait)
        {
            _thread = new Thread(() =>
            {
                _calling = true;
                try
                {
                    Result = wait();
                }
                catch (Exception e)
                {
                    Error = e;
                }
            });
            _thread.IsBackground = true;
            _thread.Start();
        }

        internal TResult Result { get; private set; } = default!;

        internal Exception? Error { get; private set; }

        internal bool IsBlocked() =>
            SpinWait.SpinUntil(
                () => !_thread.IsAlive || (_calling && (_thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0),
                s_deadline)
            && _thread.IsAlive;

        internal bool Join() => _thread.Join(s_deadline);
    }
}

/// <summary>
/// The members of a future engine that its public flavor declares, called through the shared interface, so the engine
/// tests read either flavor the same way.
/// </summary>
internal static class FutureEngineExtensions
{
    internal static bool Cancel(this IKafkaFutureImpl future, bool mayInterruptIfRunning) => future switch
    {
        KafkaFuture plain => plain.Cancel(mayInterruptIfRunning),
        AsyncKafkaFuture awaitable => awaitable.Cancel(mayInterruptIfRunning),
        _ => throw NotAnEngine(future),
    };

    internal static bool IsDone(this IKafkaFutureImpl future) => future switch
    {
        KafkaFuture plain => plain.IsDone(),
        AsyncKafkaFuture awaitable => awaitable.IsDone(),
        _ => throw NotAnEngine(future),
    };

    internal static bool IsCancelled(this IKafkaFutureImpl future) => future switch
    {
        KafkaFuture plain => plain.IsCancelled(),
        AsyncKafkaFuture awaitable => awaitable.IsCancelled(),
        _ => throw NotAnEngine(future),
    };

    internal static bool IsCompletedExceptionally(this IKafkaFutureImpl future) => future switch
    {
        KafkaFuture plain => plain.IsCompletedExceptionally(),
        AsyncKafkaFuture awaitable => awaitable.IsCompletedExceptionally(),
        _ => throw NotAnEngine(future),
    };

    internal static IKafkaFutureImpl<TResult> ThenApply<TResult>(this IKafkaFutureImpl future, Func<TResult> function) =>
        future switch
        {
            KafkaFuture plain => (IKafkaFutureImpl<TResult>)plain.ThenApply(function),
            AsyncKafkaFuture awaitable => (IKafkaFutureImpl<TResult>)awaitable.ThenApply(function),
            _ => throw NotAnEngine(future),
        };

    internal static IKafkaFutureImpl WhenComplete(this IKafkaFutureImpl future, Action<Exception?> action) =>
        future switch
        {
            KafkaFuture plain => (IKafkaFutureImpl)plain.WhenComplete(action),
            AsyncKafkaFuture awaitable => (IKafkaFutureImpl)awaitable.WhenComplete(action),
            _ => throw NotAnEngine(future),
        };

    internal static IKafkaFutureImpl<T> WhenCompleteAction<T>(this IKafkaFutureImpl<T> future, Action<T?, Exception?> action) =>
        future switch
        {
            KafkaFutureImpl<T> plain => plain.WhenCompleteAction(action),
            AsyncKafkaFutureImpl<T> awaitable => awaitable.WhenCompleteAction(action),
            _ => throw NotAnEngine(future),
        };

    private static ArgumentException NotAnEngine(IKafkaFutureImpl future) =>
        new ArgumentException("Not a future engine: " + future.GetType(), nameof(future));
}
