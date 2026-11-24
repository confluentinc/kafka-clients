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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Xunit;
using FutureTimeoutException = Confluent.Kafka.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>KafkaFutureTest</c>, plus the Python binding's future cases Java lacks, run against one public future
/// class through the views a derived class supplies; Java's <c>RuntimeException</c> is
/// <see cref="InvalidOperationException"/>, its <c>CompletionException</c> <see cref="AggregateException"/> and its
/// <c>CancellationException</c> <see cref="OperationCanceledException"/>.
/// </summary>
public abstract class KafkaFutureTestBase
{
    private protected static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_longWait = TimeSpan.FromMinutes(5);

    private protected abstract bool WaitsRethrowTheCancellation { get; }

    [Fact]
    public async Task TestCompleteFutures()
    {
        FutureView<int> future123 = New<int>();
        IKafkaFutureImpl<int> impl123 = future123.Impl;
        Assert.True(impl123.Complete(123));
        Assert.False(impl123.Complete(456));
        Assert.False(future123.Cancel(true));
        Assert.Equal(123, await Value(future123.Get()));
        AssertIsSuccessful(future123);

        FutureView<int> future456 = CompletedFuture(456);
        Assert.False(future456.Impl.Complete(789));
        Assert.False(future456.Cancel(true));
        Assert.Equal(456, await Value(future456.Get()));
        AssertIsSuccessful(future456);
    }

    [Fact]
    public async Task TestCompleteFuturesExceptionally()
    {
        FutureView<int> futureFail = New<int>();
        IKafkaFutureImpl<int> failImpl = futureFail.Impl;
        Assert.True(failImpl.CompleteWithError(new InvalidOperationException("We require more vespene gas")));
        AssertIsFailed(futureFail);
        Assert.False(failImpl.CompleteWithError(new InvalidOperationException("We require more minerals")));
        Assert.False(futureFail.Cancel(true));
        Exception thrown = FutureAssert.Cause<InvalidOperationException>(await Thrown(futureFail.Get()));
        Assert.Equal("We require more vespene gas", thrown.Message);

        FutureView<int> tricky1 = New<int>();
        IKafkaFutureImpl<int> trickyImpl = tricky1.Impl;
        var tricky = new AggregateException(new OperationCanceledException());
        Assert.True(trickyImpl.CompleteWithError(tricky));
        AssertIsFailed(tricky1);
        Assert.Same(tricky, await AwaitAndAssertFailure(tricky1, typeof(AggregateException), tricky.Message));
    }

    [Fact]
    public async Task TestCompleteFuturesViaCancellation()
    {
        FutureView<int> viaCancel = New<int>();
        Assert.True(viaCancel.Cancel(true));
        await AssertIsCancelled(viaCancel);
        await AwaitAndAssertCancelled(viaCancel, null);

        FutureView<int> viaCancellationException = New<int>();
        IKafkaFutureImpl<int> viaExceptionImpl = viaCancellationException.Impl;
        Assert.True(viaExceptionImpl.CompleteWithError(new OperationCanceledException("We require more vespene gas")));
        await AssertIsCancelled(viaCancellationException);
        await AwaitAndAssertCancelled(viaCancellationException, "We require more vespene gas");
    }

    [Fact]
    public void TestToString()
    {
        FutureView<int> success = New<int>();
        IKafkaFutureImpl<int> successImpl = success.Impl;
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", success.Describe());
        successImpl.Complete(12);
        Assert.Equal("KafkaFuture{value=12,exception=null,done=true}", success.Describe());

        FutureView<int> failure = New<int>();
        IKafkaFutureImpl<int> failureImpl = failure.Impl;
        failureImpl.CompleteWithError(new InvalidOperationException("foo"));
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.InvalidOperationException: foo,done=true}", failure.Describe());

        FutureView<int> nullMessage = New<int>();
        IKafkaFutureImpl<int> nullMessageImpl = nullMessage.Impl;
        nullMessageImpl.CompleteWithError(new KafkaException());
        Assert.Equal(
            "KafkaFuture{value=null,exception=Confluent.Kafka.Common.KafkaException,done=true}",
            nullMessage.Describe());

        FutureView<int> tricky1 = New<int>();
        IKafkaFutureImpl<int> trickyImpl = tricky1.Impl;
        var tricky = new AggregateException(new OperationCanceledException());
        trickyImpl.CompleteWithError(tricky);
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.AggregateException: " + tricky.Message + ",done=true}",
            tricky1.Describe());

        FutureView<int> cancelled = New<int>();
        cancelled.Cancel(true);
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.OperationCanceledException: "
                + new OperationCanceledException().Message + ",done=true}",
            cancelled.Describe());
    }

    [Fact]
    public async Task TestCompletingFutures()
    {
        FutureView<string> future = New<string>();
        IKafkaFutureImpl<string> impl = future.Impl;
        var myThread = new CompleterThread<string>(impl, "You must construct additional pylons.");
        AssertIsNotCompleted(future);
        Assert.Equal("I am ready", future.GetNow("I am ready"));
        myThread.Start();
        await AwaitAndAssertResult(future, "You must construct additional pylons.", "I am ready");
        AssertIsSuccessful(future);
        myThread.Join();
        Assert.Null(myThread.TestException);
    }

    [Fact]
    public async Task TestCompletingFuturesExceptionally()
    {
        FutureView<string> future = New<string>();
        IKafkaFutureImpl<string> impl = future.Impl;
        var myThread = new CompleterThread<string>(
            impl, null!, new InvalidOperationException("Ultimate efficiency achieved."));
        AssertIsNotCompleted(future);
        Assert.Equal("I am ready", future.GetNow("I am ready"));
        myThread.Start();
        await AwaitAndAssertFailure(future, typeof(InvalidOperationException), "Ultimate efficiency achieved.");
        AssertIsFailed(future);
        myThread.Join();
        Assert.Null(myThread.TestException);
    }

    [Fact]
    public async Task TestCompletingFuturesViaCancellation()
    {
        FutureView<string> future = New<string>();
        IKafkaFutureImpl<string> impl = future.Impl;
        var myThread = new CompleterThread<string>(
            impl, null!, new OperationCanceledException("Ultimate efficiency achieved."));
        AssertIsNotCompleted(future);
        Assert.Equal("I am ready", future.GetNow("I am ready"));
        myThread.Start();
        await AwaitAndAssertCancelled(future, "Ultimate efficiency achieved.");
        await AssertIsCancelled(future);
        myThread.Join();
        Assert.Null(myThread.TestException);
    }

    [Fact]
    public void TestThenApplyOnSucceededFuture()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        FutureView<int> doubledFuture = future.ThenApply(integer => 2 * integer);
        Assert.False(doubledFuture.IsDone);
        FutureView<int> tripledFuture = future.ThenApply(integer => 3 * integer);
        Assert.False(tripledFuture.IsDone);
        impl.Complete(21);
        Assert.Equal(21, future.GetNow(-1));
        Assert.Equal(42, doubledFuture.GetNow(-1));
        Assert.Equal(63, tripledFuture.GetNow(-1));
        FutureView<int> quadrupledFuture = future.ThenApply(integer => 4 * integer);
        Assert.Equal(84, quadrupledFuture.GetNow(-1));
    }

    [Fact]
    public async Task TestThenApplyOnFailedFuture()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        FutureView<int> dependantFuture = future.ThenApply(integer => 2 * integer);
        impl.CompleteWithError(new InvalidOperationException("We require more vespene gas"));
        AssertIsFailed(future);
        AssertIsFailed(dependantFuture);
        Exception failure = await AwaitAndAssertFailure(
            future, typeof(InvalidOperationException), "We require more vespene gas");
        Assert.Same(
            failure,
            await AwaitAndAssertFailure(
                dependantFuture, typeof(InvalidOperationException), "We require more vespene gas"));
    }

    [Fact]
    public async Task TestThenApplyOnFailedFutureTricky()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        FutureView<int> dependantFuture = future.ThenApply(integer => 2 * integer);
        var tricky = new AggregateException(new InvalidOperationException("We require more vespene gas"));
        impl.CompleteWithError(tricky);
        AssertIsFailed(future);
        AssertIsFailed(dependantFuture);
        Assert.Same(tricky, await AwaitAndAssertFailure(future, typeof(AggregateException), tricky.Message));
        Assert.Same(tricky, await AwaitAndAssertFailure(dependantFuture, typeof(AggregateException), tricky.Message));
    }

    [Fact]
    public async Task TestThenApplyOnFailedFutureTricky2()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        FutureView<int> dependantFuture = future.ThenApply(integer => 2 * integer);
        var tricky = new AggregateException(new OperationCanceledException());
        impl.CompleteWithError(tricky);
        AssertIsFailed(future);
        AssertIsFailed(dependantFuture);
        Assert.Same(tricky, await AwaitAndAssertFailure(future, typeof(AggregateException), tricky.Message));
        Assert.Same(tricky, await AwaitAndAssertFailure(dependantFuture, typeof(AggregateException), tricky.Message));
    }

    [Fact]
    public async Task TestThenApplyOnSucceededFutureAndFunctionThrows()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        FutureView<int> dependantFuture = future.ThenApply<int>(
            integer => throw new InvalidOperationException("We require more vespene gas"));
        impl.Complete(21);
        AssertIsSuccessful(future);
        AssertIsFailed(dependantFuture);
        await AwaitAndAssertResult(future, 21, -1);
        await AwaitAndAssertFailure(dependantFuture, typeof(InvalidOperationException), "We require more vespene gas");
    }

    [Fact]
    public async Task TestThenApplyOnSucceededFutureAndFunctionThrowsCompletionException()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        var thrown = new AggregateException(new InvalidOperationException("We require more vespene gas"));
        FutureView<int> dependantFuture = future.ThenApply<int>(integer => throw thrown);
        impl.Complete(21);
        AssertIsSuccessful(future);
        AssertIsFailed(dependantFuture);
        await AwaitAndAssertResult(future, 21, -1);
        Exception cause = await AwaitAndAssertFailure(dependantFuture, typeof(AggregateException), thrown.Message);
        Assert.IsType<InvalidOperationException>(cause.InnerException);
        Assert.Equal("We require more vespene gas", cause.InnerException!.Message);
    }

    [Fact]
    public async Task TestThenApplyOnFailedFutureFunctionNotCalled()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        bool ran = false;
        FutureView<int> dependantFuture = future.ThenApply(integer =>
        {
            ran = true;
            return 0;
        });
        impl.CompleteWithError(new InvalidOperationException("We require more minerals"));
        AssertIsFailed(future);
        AssertIsFailed(dependantFuture);
        await AwaitAndAssertFailure(future, typeof(InvalidOperationException), "We require more minerals");
        await AwaitAndAssertFailure(dependantFuture, typeof(InvalidOperationException), "We require more minerals");
        Assert.False(ran);
    }

    [Fact]
    public async Task TestThenApplyOnCancelledFuture()
    {
        FutureView<int> future = New<int>();
        FutureView<int> dependantFuture = future.ThenApply(integer => 2 * integer);
        future.Cancel(true);
        await AssertIsCancelled(future);
        await AssertIsCancelled(dependantFuture);
        await AwaitAndAssertCancelled(future, null);
        await AwaitAndAssertCancelled(dependantFuture, null);
    }

    [Fact]
    public void TestWhenCompleteOnSucceededFuture()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        int seenValue = -1;
        Exception? seenError = null;
        bool ran = false;
        FutureView<int> dependantFuture = future.WhenComplete((integer, ex) =>
        {
            ran = true;
            seenValue = integer;
            seenError = ex;
        });
        Assert.False(dependantFuture.IsDone);
        Assert.True(impl.Complete(21));
        Assert.True(ran);
        Assert.Equal(21, seenValue);
        Assert.Null(seenError);
    }

    [Fact]
    public void TestWhenCompleteOnFailedFuture()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        int seenValue = -1;
        Exception? err = null;
        bool ran = false;
        FutureView<int> dependantFuture = future.WhenComplete((integer, ex) =>
        {
            ran = true;
            err = ex;
            seenValue = integer;
        });
        Assert.False(dependantFuture.IsDone);
        var ex = new InvalidOperationException("We require more vespene gas");
        Assert.True(impl.CompleteWithError(ex));
        Assert.True(ran);
        Assert.Same(ex, err);
        Assert.Equal(0, seenValue);
    }

    [Fact]
    public async Task TestWhenCompleteOnSucceededFutureAndConsumerThrows()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        bool ran = false;
        FutureView<int> dependantFuture = future.WhenComplete((integer, ex) =>
        {
            ran = true;
            throw new InvalidOperationException("We require more minerals");
        });
        Assert.False(dependantFuture.IsDone);
        Assert.True(impl.Complete(21));
        AssertIsSuccessful(future);
        Assert.True(ran);
        AssertIsFailed(dependantFuture);
        await AwaitAndAssertFailure(dependantFuture, typeof(InvalidOperationException), "We require more minerals");
    }

    [Fact]
    public async Task TestWhenCompleteOnFailedFutureAndConsumerThrows()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        bool ran = false;
        FutureView<int> dependantFuture = future.WhenComplete((integer, ex) =>
        {
            ran = true;
            throw new InvalidOperationException("We require more minerals");
        });
        Assert.False(dependantFuture.IsDone);
        Assert.True(impl.CompleteWithError(new InvalidOperationException("We require more vespene gas")));
        AssertIsFailed(future);
        Assert.True(ran);
        AssertIsFailed(dependantFuture);
        await AwaitAndAssertFailure(dependantFuture, typeof(InvalidOperationException), "We require more vespene gas");
    }

    [Fact]
    public void TestWhenCompleteOnCancelledFuture()
    {
        FutureView<int> future = New<int>();
        int seenValue = -1;
        Exception? err = null;
        bool ran = false;
        FutureView<int> dependantFuture = future.WhenComplete((integer, ex) =>
        {
            ran = true;
            err = ex;
            seenValue = integer;
        });
        Assert.False(dependantFuture.IsDone);
        Assert.True(future.Cancel(true));
        Assert.True(ran);
        Assert.IsAssignableFrom<OperationCanceledException>(err);
        Assert.Equal(0, seenValue);
    }

    [Fact]
    public async Task TestAllOfFutures()
    {
        const int NumThreads = 5;
        var impls = new IKafkaFutureImpl<int>[NumThreads];
        var futures = new FutureView<int>[NumThreads];
        for (int i = 0; i < NumThreads; i++)
        {
            futures[i] = New<int>();
            impls[i] = futures[i].Impl;
        }

        FutureView allFuture = AllOf(futures);
        var completerThreads = new CompleterThread<int>[NumThreads];
        for (int i = 0; i < NumThreads; i++)
        {
            completerThreads[i] = new CompleterThread<int>(impls[i], i);
        }

        Assert.False(allFuture.IsDone);
        var waiters = new Task<int>[NumThreads];
        for (int i = 0; i < NumThreads; i++)
        {
            waiters[i] = futures[i].Get();
        }

        for (int i = 0; i < NumThreads - 1; i++)
        {
            completerThreads[i].Start();
        }

        Assert.False(allFuture.IsDone);
        completerThreads[NumThreads - 1].Start();
        await Returned(allFuture.Wait());
        AssertIsSuccessful(allFuture);
        for (int i = 0; i < NumThreads; i++)
        {
            Assert.Equal(i, await Value(futures[i].Get()));
        }

        for (int i = 0; i < NumThreads; i++)
        {
            completerThreads[i].Join();
            Assert.Null(completerThreads[i].TestException);
            Assert.Equal(i, await Value(waiters[i]));
        }
    }

    [Fact]
    public async Task TestAllOfFuturesWithFailure()
    {
        const int NumThreads = 5;
        var impls = new IKafkaFutureImpl<int>[NumThreads];
        var futures = new FutureView<int>[NumThreads];
        for (int i = 0; i < NumThreads; i++)
        {
            futures[i] = New<int>();
            impls[i] = futures[i].Impl;
        }

        FutureView allFuture = AllOf(futures);
        int lastIndex = NumThreads - 1;
        var completerThreads = new CompleterThread<int>[NumThreads];
        for (int i = 0; i < lastIndex; i++)
        {
            completerThreads[i] = new CompleterThread<int>(impls[i], i);
        }

        completerThreads[lastIndex] = new CompleterThread<int>(
            impls[lastIndex], 0, new InvalidOperationException("Last one failed"));
        Assert.False(allFuture.IsDone);
        var waiters = new Task<int>[NumThreads];
        for (int i = 0; i < NumThreads; i++)
        {
            waiters[i] = futures[i].Get();
        }

        for (int i = 0; i < lastIndex; i++)
        {
            completerThreads[i].Start();
        }

        Assert.False(allFuture.IsDone);
        completerThreads[lastIndex].Start();
        await AwaitAndAssertFailure(allFuture, typeof(InvalidOperationException), "Last one failed");
        AssertIsFailed(allFuture);
        for (int i = 0; i < lastIndex; i++)
        {
            Assert.Equal(i, await Value(futures[i].Get()));
        }

        AssertIsFailed(futures[lastIndex]);
        for (int i = 0; i < NumThreads; i++)
        {
            completerThreads[i].Join();
            Assert.Null(completerThreads[i].TestException);
            if (i == lastIndex)
            {
                Exception waiterException = FutureAssert.Cause<InvalidOperationException>(await Thrown(waiters[i]));
                Assert.Equal("Last one failed", waiterException.Message);
            }
            else
            {
                Assert.Equal(i, await Value(waiters[i]));
            }
        }
    }

    [Fact]
    public async Task TestAllOfFuturesHandlesZeroFutures()
    {
        FutureView allFuture = AllOf();
        Assert.True(allFuture.IsDone);
        Assert.False(allFuture.IsCancelled);
        Assert.False(allFuture.IsCompletedExceptionally);
        await Returned(allFuture.Wait());
    }

    [Fact]
    public async Task TestFutureTimeoutWithZeroWait()
    {
        FutureView<string> future = New<string>();
        Assert.Equal(string.Empty, Assert.IsType<FutureTimeoutException>(await Thrown(future.Get(TimeSpan.Zero))).Message);
    }

    [Fact]
    public async Task TestGetTimeoutReading()
    {
        FutureView<string> future = New<string>();
        TimeSpan[] timeouts =
        {
            TimeSpan.Zero,
            TimeSpan.FromSeconds(-1),
            TimeSpan.FromSeconds(-0.5),
            TimeSpan.FromSeconds(-3),
            TimeSpan.FromMinutes(-10),
            TimeSpan.FromTicks(4_000),
        };
        foreach (TimeSpan timeout in timeouts)
        {
            Exception thrown = Assert.IsType<FutureTimeoutException>(await Thrown(future.Get(timeout)));
            Assert.Equal(string.Empty, thrown.Message);
        }

        Assert.False(future.IsDone);
        FutureView<string> done = CompletedFuture("v");
        Assert.Equal("v", await Value(done.Get(TimeSpan.Zero)));
        Assert.Equal("v", await Value(done.Get(TimeSpan.FromSeconds(-1))));
        Assert.Equal("v", await Value(done.Get(TimeSpan.FromMilliseconds(5))));
    }

    [Fact]
    public async Task TestCancellingADependantLeavesItsSource()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        FutureView<int> dependant = future.ThenApply(value => value);
        Assert.True(dependant.Cancel(false));
        Assert.True(dependant.IsCancelled);
        Assert.False(future.IsDone);
        Assert.True(impl.Complete(3));
        Assert.Equal(3, await Value(future.Get()));
        Assert.True(dependant.IsCancelled);
    }

    [Fact]
    public async Task TestAllOfACancelledSourceIsCancelled()
    {
        FutureView<int> a = New<int>();
        FutureView<int> b = New<int>();
        IKafkaFutureImpl<int> bImpl = b.Impl;
        FutureView allFuture = AllOf(a, b);
        a.Cancel(false);
        Assert.False(allFuture.IsDone);
        bImpl.Complete(1);
        await AssertIsCancelled(allFuture);
    }

    [Fact]
    public async Task TestWaitingTaskCancellationLeavesTheFuture()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        using var cancellation = new CancellationTokenSource();
        Task<int> wait = future.Get(cancellation.Token);
        cancellation.Cancel();
        OperationCanceledException thrown = Assert.IsAssignableFrom<OperationCanceledException>(await Thrown(wait));
        Assert.Equal(cancellation.Token, thrown.CancellationToken);
        Assert.False(future.IsDone);
        Assert.False(future.IsCancelled);
        Assert.True(impl.Complete(1));
        Assert.Equal(1, await Value(future.Get()));
    }

    [Fact]
    public async Task TestEveryListenerRunsWhenOneRaises()
    {
        FutureView<int> future = New<int>();
        IKafkaFutureImpl<int> impl = future.Impl;
        var raised = new InvalidOperationException("The function raised.");
        FutureView<int> d1 = future.ThenApply<int>(value => throw raised);
        FutureView<int> d2 = future.ThenApply(value => value + 1);
        FutureView<int> innerD2 = future.ThenApply(value => value * 3);
        FutureView allFuture = AllOf(d1, d2);
        Assert.True(impl.Complete(1));
        Assert.Equal(2, d2.GetNow(-1));
        Assert.Same(raised, FutureAssert.Throws<InvalidOperationException>(() => d1.GetNow(-1)));
        Assert.True(allFuture.IsDone);
        Assert.Equal(3, await Value(innerD2.Get()));
    }

    private protected static async Task<T> Value<T>(Task<T> wait)
    {
        await Settled(wait).ConfigureAwait(false);
        return await wait.ConfigureAwait(false);
    }

    private protected static async Task Returned(Task wait)
    {
        await Settled(wait).ConfigureAwait(false);
        await wait.ConfigureAwait(false);
    }

    private protected static async Task<Exception> Thrown(Task wait)
    {
        await Settled(wait).ConfigureAwait(false);
        Exception? error = null;
        try
        {
            await wait.ConfigureAwait(false);
        }
        catch (Exception thrown)
        {
            error = thrown;
        }

        Assert.NotNull(error);
        return error;
    }

    private protected abstract FutureView<T> New<T>();

    private protected abstract FutureView<T> CompletedFuture<T>(T value);

    private protected abstract FutureView AllOf(params FutureView<int>[] futures);

    private static async Task Settled(Task wait) =>
        Assert.True(
            ReferenceEquals(wait, await Task.WhenAny(wait, Task.Delay(s_deadline)).ConfigureAwait(false)),
            $"The wait did not end within {s_deadline}.");

    private static void AssertIsSuccessful(FutureView future)
    {
        Assert.True(future.IsDone);
        Assert.False(future.IsCompletedExceptionally);
        Assert.False(future.IsCancelled);
    }

    private static void AssertIsFailed(FutureView future)
    {
        Assert.True(future.IsDone);
        Assert.False(future.IsCancelled);
        Assert.True(future.IsCompletedExceptionally);
    }

    private static void AssertIsNotCompleted(FutureView future)
    {
        Assert.False(future.IsDone);
        Assert.False(future.IsCompletedExceptionally);
        Assert.False(future.IsCancelled);
    }

    private static async Task AssertIsCancelled(FutureView future)
    {
        Assert.True(future.IsDone);
        Assert.True(future.IsCancelled);
        Assert.True(future.IsCompletedExceptionally);
        Assert.ThrowsAny<OperationCanceledException>(future.WaitNow);
        Assert.IsAssignableFrom<OperationCanceledException>(await Thrown(future.Wait(TimeSpan.Zero)));
    }

    private static async Task AwaitAndAssertResult<T>(FutureView<T> future, T expectedResult, T alternativeValue)
    {
        Assert.NotEqual(expectedResult, alternativeValue);
        Assert.Equal(expectedResult, await Value(future.Get(s_longWait)));
        Assert.Equal(expectedResult, await Value(future.Get()));
        Assert.Equal(expectedResult, future.GetNow(alternativeValue));
    }

    private static async Task<Exception> AwaitAndAssertFailure(
        FutureView future, Type expectedException, string expectedMessage)
    {
        Exception timed = FutureAssert.Cause<Exception>(await Thrown(future.Wait(s_longWait)), expectedException);
        Assert.Equal(expectedMessage, timed.Message);
        Assert.Same(timed, FutureAssert.Cause<Exception>(await Thrown(future.Wait()), expectedException));
        Exception now = FutureAssert.Cause<Exception>(Assert.ThrowsAny<Exception>(future.WaitNow), expectedException);
        Assert.Same(timed, now);
        return now;
    }

    private async Task AwaitAndAssertCancelled(FutureView future, string? expectedMessage)
    {
        string message = expectedMessage ?? new OperationCanceledException().Message;
        AssertCancelledWait(await Thrown(future.Wait(s_longWait)), message);
        AssertCancelledWait(await Thrown(future.Wait()), message);
        OperationCanceledException now = Assert.Throws<OperationCanceledException>(future.WaitNow);
        Assert.Equal(message, now.Message);
    }

    private void AssertCancelledWait(Exception thrown, string message)
    {
        if (WaitsRethrowTheCancellation)
        {
            Assert.IsType<OperationCanceledException>(thrown);
            Assert.Equal(message, thrown.Message);
        }
        else
        {
            Assert.IsType<TaskCanceledException>(thrown);
        }
    }

    /// <summary>Java's <c>CompleterThread</c>: completes a future from a thread of its own.</summary>
    private sealed class CompleterThread<T>
    {
        private readonly Thread _thread;

        internal CompleterThread(IKafkaFutureImpl<T> future, T value, Exception? exception = null)
        {
            _thread = new Thread(() =>
            {
                try
                {
                    Thread.Yield();
                    if (exception is null)
                    {
                        future.Complete(value);
                    }
                    else
                    {
                        future.CompleteWithError(exception);
                    }
                }
                catch (Exception testException)
                {
                    TestException = testException;
                }
            });
            _thread.IsBackground = true;
        }

        internal Exception? TestException { get; private set; }

        internal void Start() => _thread.Start();

        internal void Join() => Assert.True(_thread.Join(s_deadline), $"The completer did not finish within {s_deadline}.");
    }
}

/// <summary>One public future as the shared tests see it, every wait returned as a task.</summary>
internal abstract class FutureView
{
    internal abstract bool IsDone { get; }

    internal abstract bool IsCancelled { get; }

    internal abstract bool IsCompletedExceptionally { get; }

    internal abstract bool Cancel(bool mayInterruptIfRunning);

    internal abstract Task Wait();

    internal abstract Task Wait(TimeSpan timeout);

    internal abstract void WaitNow();

    internal abstract string Describe();
}

/// <summary>One public future with a value, as the shared tests see it, and its engine.</summary>
internal abstract class FutureView<T> : FutureView
{
    internal abstract IKafkaFutureImpl<T> Impl { get; }

    internal abstract Task<T> Get();

    internal abstract Task<T> Get(TimeSpan timeout);

    internal abstract Task<T> Get(CancellationToken cancellationToken);

    internal abstract T GetNow(T valueIfAbsent);

    internal abstract FutureView<TR> ThenApply<TR>(Func<T, TR> function);

    internal abstract FutureView<T> WhenComplete(Action<T?, Exception?> action);

    internal override Task Wait() => Get();

    internal override Task Wait(TimeSpan timeout) => Get(timeout);

    internal override void WaitNow() => _ = GetNow(default!);
}
