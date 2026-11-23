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
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

using Confluent.Kafka.Producer;
using Xunit;

using FutureTimeoutException = Confluent.Kafka.TimeoutException;
using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="AsyncKafkaFuture{T}"/>, which its engine <see cref="AsyncKafkaFutureImpl{T}"/> is: a
/// <c>default</c> one is <see langword="null"/>, the same-instance contract of <c>Get()</c>, <c>ConfigureAwait</c> on the
/// task, identity equality, the future seen as an <see cref="AsyncKafkaFuture"/>, and the member set.
/// </summary>
public sealed class PublicAsyncKafkaFutureTests
{
    [Fact]
    public void Default_IsNull()
    {
        AsyncKafkaFuture<RecordMetadata>? declared = default;

        Assert.Null(declared);
        Assert.Null(default(AsyncKafkaFuture<RecordMetadata>));
        Assert.Null(default(AsyncKafkaFuture<long>));
        AsyncKafkaFuture<int>?[] slots = new AsyncKafkaFuture<int>?[1];
        Assert.Null(slots[0]);
    }

    [Fact]
    public async Task DefaultValueTask_AwaitedFuture_IsNull()
    {
        ValueTask<AsyncKafkaFuture<RecordMetadata>> send = default(ValueTask<AsyncKafkaFuture<RecordMetadata>>);
        Assert.True(send.IsCompletedSuccessfully);

        AsyncKafkaFuture<RecordMetadata>? future = await send;

        Assert.Null(future);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void Get_ReturnsTheSameTaskOnEveryCall(string state)
    {
        AsyncKafkaFutureImpl<RecordMetadata> delivery = DeliveryIn(state);
        AsyncKafkaFuture<RecordMetadata> future = delivery;

        Task<RecordMetadata> first = future.Get();
        Task<RecordMetadata> second = future.Get();

        Assert.Same(((AsyncKafkaFuture)delivery).Get(), first);
        Assert.Same(first, second);
        if (state == "completed")
        {
            // A value has nothing to wrap: the task is the raw one.
            Assert.Same(delivery.ToCompletionStage(), first);
        }
        else
        {
            // A delivery cannot be cancelled, so a cancellation given as its error is a failure like any other.
            Assert.NotSame(delivery.ToCompletionStage(), first);
        }

        _ = first.Exception;
    }

    [Fact]
    public async Task Get_ConfigureAwait_FlowsThroughTheTask()
    {
        RecordMetadata metadata = NewMetadata();
        AsyncKafkaFutureImpl<RecordMetadata> completion = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = completion;

        Task<RecordMetadata> awaiting = AwaitGetWithoutContext(future);
        Assert.False(awaiting.IsCompleted);
        Assert.True(completion.Complete(metadata));

        Assert.Same(metadata, await awaiting);
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheImpl()
    {
        AsyncKafkaFutureImpl<RecordMetadata> impl = Delivered(NewMetadata());
        AsyncKafkaFuture<RecordMetadata> a = impl;
        AsyncKafkaFuture<RecordMetadata> sameImpl = impl;
        Assert.True(a.Equals(sameImpl));
        Assert.True(a == sameImpl);
        Assert.False(a != sameImpl);
        Assert.Equal(a.GetHashCode(), sameImpl.GetHashCode());

        RecordMetadata shared = NewMetadata();
        AsyncKafkaFutureImpl<RecordMetadata> firstImpl = Delivered(shared);
        AsyncKafkaFutureImpl<RecordMetadata> secondImpl = Delivered(shared);
        Assert.NotSame(firstImpl, secondImpl);
        AsyncKafkaFuture<RecordMetadata> x = firstImpl;
        AsyncKafkaFuture<RecordMetadata> y = secondImpl;
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        AsyncKafkaFuture<RecordMetadata>? n1 = null;
        AsyncKafkaFuture<RecordMetadata>? n2 = default;
        Assert.True(n1 == n2);
        Assert.False(n1 != n2);
        Assert.True(Equals(n1, n2));
        Assert.False(a.Equals(n1));
        Assert.True(n1 != a);
        Assert.True(a != n1);
        Assert.False(n1 == a);
        Assert.False(a == n1);

        object boxedEqual = sameImpl;
        object boxedDifferent = y;
        object rawImpl = impl;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.Same(rawImpl, a);
        Assert.True(a.Equals(rawImpl));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void Null_EqualityOperators_AreNullSafe_TypedAndPlain()
    {
        AsyncKafkaFuture<RecordMetadata>? none = null;
        AsyncKafkaFuture? plainNone = none;
        AsyncKafkaFuture<RecordMetadata> live = new AsyncKafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture plainLive = live;

        Assert.True(none == null);
        Assert.False(none != null);
        Assert.True(plainNone == null);
        Assert.True(plainNone == none);
        Assert.False(live == null);
        Assert.True(live != null);
        Assert.False(plainLive == null);
        Assert.False(plainLive == none);
        Assert.False(none == plainLive);
        Assert.False(live.Equals(none));
        Assert.False(plainLive.Equals(plainNone));
        Assert.False(live.Equals((object?)null));
    }

    [Fact]
    public void ThenApplyAndWhenComplete_RejectANullDelegate()
    {
        AsyncKafkaFuture<RecordMetadata>[] futures =
        {
            new AsyncKafkaFutureImpl<RecordMetadata>(),
            AsyncKafkaFuture.CompletedFuture(NewMetadata()),
        };
        foreach (AsyncKafkaFuture<RecordMetadata> future in futures)
        {
            Assert.Equal(
                "function",
                Assert.Throws<ArgumentNullException>(() => future.ThenApply<int>(null!)).ParamName);
            Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => future.WhenComplete(null!)).ParamName);
        }
    }

    [Fact]
    public void ThenApplyWhenCompleteAndTheStatics_ReturnTheAsyncEngine_TypedAndPlain()
    {
        AsyncKafkaFuture<int> typed = new AsyncKafkaFutureImpl<int>();
        AsyncKafkaFuture plain = typed;

        Assert.IsType<AsyncKafkaFutureImpl<int>>(typed.ThenApply(value => value + 1));
        Assert.IsType<AsyncKafkaFutureImpl<int>>(typed.WhenComplete((value, error) => { }));
        Assert.IsType<AsyncKafkaFutureImpl<string>>(plain.ThenApply(() => "applied"));
        Assert.IsType<AsyncKafkaFutureImpl<int>>(plain.WhenComplete(error => { }));
        Assert.IsType<AsyncKafkaFutureImpl<int>>(AsyncKafkaFuture.CompletedFuture(1));
        Assert.IsType<AsyncKafkaFutureImpl<object?>>(AsyncKafkaFuture.AllOf(typed));
        Assert.IsType<AsyncKafkaFutureImpl<object?>>(AsyncKafkaFuture.AllOf<int>(typed));
    }

    [Theory]
    [InlineData("succeeded")]
    [InlineData("failed")]
    public void OnceDone_ThenApplyWhenCompleteAndAllOf_RunInsideTheCall(string outcome)
    {
        RecordMetadata metadata = NewMetadata();
        InvalidOperationException failure = new InvalidOperationException("delivery failed");
        AsyncKafkaFutureImpl<RecordMetadata> delivery = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = delivery;
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Assert.True(outcome == "succeeded" ? delivery.Complete(metadata) : delivery.CompleteWithError(failure));
        cancellation.Cancel();
        int caller = Environment.CurrentManagedThreadId;
        int appliedOn = 0;
        int completedOn = 0;
        RecordMetadata? seenValue = null;
        Exception? seenError = null;

        AsyncKafkaFuture<long> applied = future.ThenApply(value =>
        {
            appliedOn = Environment.CurrentManagedThreadId;
            return value.Offset();
        });
        AsyncKafkaFuture<RecordMetadata> completed = future.WhenComplete((value, error) =>
        {
            completedOn = Environment.CurrentManagedThreadId;
            seenValue = value;
            seenError = error;
        });
        AsyncKafkaFuture all = AsyncKafkaFuture.AllOf(AsyncKafkaFuture.CompletedFuture(1), future);

        Assert.True(applied.IsDone());
        Assert.True(completed.IsDone());
        Assert.True(all.IsDone());
        Assert.Equal(caller, completedOn);
        Assert.Same(future.Get(), future.Get(TimeSpan.Zero));
        Assert.Same(future.Get(), future.Get(TimeSpan.FromSeconds(1)));
        Assert.Same(future.Get(), future.Get(cancellation.Token));
        if (outcome == "succeeded")
        {
            Assert.Equal(caller, appliedOn);
            Assert.Equal(metadata.Offset(), applied.GetNow(-1));
            Assert.Same(metadata, seenValue);
            Assert.Null(seenError);
            Assert.Same(metadata, completed.GetNow(NewMetadata()));
            all.GetNow();
            Assert.False(all.IsCompletedExceptionally());
        }
        else
        {
            Assert.Equal(0, appliedOn);
            AssertFailsWith(failure, () => applied.GetNow(-1));
            Assert.Null(seenValue);
            Assert.Same(failure, seenError);
            AssertFailsWith(failure, () => completed.GetNow(NewMetadata()));
            AssertFailsWith(failure, () => all.GetNow());
            _ = future.Get().Exception;
        }
    }

    [Fact]
    public async Task OverAPendingDelivery_TheTimedGet_AnswersFromTheTask()
    {
        RecordMetadata metadata = NewMetadata();
        InvalidOperationException failure = new InvalidOperationException("delivery failed");
        AsyncKafkaFutureImpl<RecordMetadata> delivery = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = delivery;

        Assert.Same(future.Get(), future.Get(Timeout.InfiniteTimeSpan));
        await AssertTimesOut(future.Get(TimeSpan.Zero));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(-5)));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(50)));

        Task<RecordMetadata> waiting = future.Get(BlockingCall.Deadline);
        Assert.False(waiting.IsCompleted);
        Assert.True(delivery.Complete(metadata));
        Assert.Same(metadata, await TestTimeout.Run(() => waiting, BlockingCall.Deadline));

        AsyncKafkaFutureImpl<RecordMetadata> failing = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        Task<RecordMetadata> waitingForFailure = failing.Get(BlockingCall.Deadline);
        Assert.False(waitingForFailure.IsCompleted);
        Assert.True(failing.CompleteWithError(failure));
        InvalidOperationException thrown = await FutureAssert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => waitingForFailure, BlockingCall.Deadline));
        Assert.Same(failure, thrown);
        Assert.Equal("delivery failed", thrown.Message);
    }

    [Fact]
    public async Task OverAPendingDelivery_TheCancellableGet_AnswersFromTheTask()
    {
        RecordMetadata metadata = NewMetadata();
        AsyncKafkaFutureImpl<RecordMetadata> delivery = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = delivery;
        using CancellationTokenSource preCanceled = new CancellationTokenSource();
        preCanceled.Cancel();
        using CancellationTokenSource firing = new CancellationTokenSource();
        using CancellationTokenSource unfired = new CancellationTokenSource();

        Assert.Same(future.Get(), future.Get(CancellationToken.None));
        Task<RecordMetadata> canceledAtOnce = future.Get(preCanceled.Token);
        Assert.True(canceledAtOnce.IsCanceled);
        await AssertCanceledBy(preCanceled.Token, canceledAtOnce);

        Task<RecordMetadata> canceledLater = future.Get(firing.Token);
        Assert.False(canceledLater.IsCompleted);
        firing.Cancel();
        await AssertCanceledBy(firing.Token, canceledLater);
        Assert.False(delivery.IsDone());

        Task<RecordMetadata> waiting = future.Get(unfired.Token);
        Assert.False(waiting.IsCompleted);
        Assert.True(delivery.Complete(metadata));
        Assert.Same(metadata, await TestTimeout.Run(() => waiting, BlockingCall.Deadline));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void OverADelivery_StateReadsFollowIt_AndCancelChangesNothing(string state)
    {
        RecordMetadata metadata = NewMetadata();
        InvalidOperationException failure = new InvalidOperationException("delivery failed");
        OperationCanceledException cancellation = new OperationCanceledException("The delivery was canceled.");
        AsyncKafkaFutureImpl<RecordMetadata> delivery = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        switch (state)
        {
            case "completed":
                Assert.True(delivery.Complete(metadata));
                break;
            case "faulted":
                Assert.True(delivery.CompleteWithError(failure));
                break;
            case "canceled":
                Assert.True(delivery.CompleteWithError(cancellation));
                break;
        }

        AsyncKafkaFuture<RecordMetadata> future = delivery;
        RecordMetadata absent = NewMetadata();

        Assert.False(future.Cancel(true));
        Assert.False(future.Cancel(false));
        Assert.Equal(state != "pending", future.IsDone());
        Assert.False(future.IsCancelled());
        Assert.Equal(state == "faulted" || state == "canceled", future.IsCompletedExceptionally());
        Assert.Same(future.Get(), ((AsyncKafkaFuture)future).Get());
        Assert.Same(future.ToCompletionStage(), ((AsyncKafkaFuture)future).ToCompletionStage());
        switch (state)
        {
            case "pending":
                Assert.Same(absent, future.GetNow(absent));
                Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());
                break;
            case "completed":
                Assert.Same(metadata, future.GetNow(absent));
                Assert.Equal("KafkaFuture{value=" + metadata + ",exception=null,done=true}", future.ToString());
                break;
            case "faulted":
                Assert.Same(failure, FutureAssert.Throws<InvalidOperationException>(() => future.GetNow(absent)));
                Assert.Equal(
                    "KafkaFuture{value=null,exception=System.InvalidOperationException: delivery failed,done=true}",
                    future.ToString());
                break;
            default:
                Assert.Same(cancellation, FutureAssert.Throws<OperationCanceledException>(() => future.GetNow(absent)));
                Assert.Equal(
                    "KafkaFuture{value=null,exception=System.OperationCanceledException: The delivery was canceled.,"
                        + "done=true}",
                    future.ToString());
                break;
        }

        Assert.False(future.IsCancelled());
        _ = future.Get().Exception;
    }

    [Fact]
    public async Task ToCompletionStage_IsTheSameTaskEachCall_ButNotTheTaskGetReturns()
    {
        AsyncKafkaFuture<RecordMetadata> overDelivery = DeliveryIn("pending");
        Assert.Same(overDelivery.ToCompletionStage(), overDelivery.ToCompletionStage());
        Assert.NotSame(overDelivery.Get(), overDelivery.ToCompletionStage());

        AsyncKafkaFutureImpl<RecordMetadata> impl = new AsyncKafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture<RecordMetadata> overImpl = impl;
        Task<RecordMetadata> stage = overImpl.ToCompletionStage();
        Assert.Same(stage, overImpl.ToCompletionStage());
        Assert.NotSame(overImpl.Get(), stage);

        InvalidOperationException failure = new InvalidOperationException("delivery failed");
        Assert.True(impl.CompleteWithError(failure));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => stage));
        Assert.Same(failure, await FutureAssert.ThrowsAsync<InvalidOperationException>(() => overImpl.Get()));
    }

    [Fact]
    public async Task ATypedFuture_IsItsOwnPlainFuture_TheSameObject()
    {
        AsyncKafkaFutureImpl<RecordMetadata> impl = new AsyncKafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture<RecordMetadata> typed = impl;
        AsyncKafkaFuture erased = typed;
        Assert.Same(typed, erased);
        Assert.True(erased == impl);
        Assert.Equal(typed.GetHashCode(), erased.GetHashCode());
        Assert.False(erased.IsDone());
        Task waiting = erased.Get();
        Assert.True(impl.Complete(NewMetadata()));
        await TestTimeout.Run(() => waiting, BlockingCall.Deadline);
        Assert.True(erased.IsDone());

        AsyncKafkaFutureImpl<RecordMetadata> failing = new AsyncKafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture erasedFailure = failing;
        KafkaException cause = new Common.Errors.TimeoutException("The operation failed.");
        Assert.True(failing.CompleteWithError(cause));
        Assert.Same(cause, FutureAssert.Throws<KafkaTimeoutException>(() => erasedFailure.GetNow()));
        Assert.True(erasedFailure.IsCompletedExceptionally());
    }

    [Fact]
    public void Equality_BetweenATypedFutureAndItsPlainView_IsSymmetric()
    {
        AsyncKafkaFuture<RecordMetadata> overImpl = new AsyncKafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture<RecordMetadata> otherImpl = new AsyncKafkaFutureImpl<RecordMetadata>();

        AssertTypedAndErasedEquality(overImpl, overImpl, equal: true);
        AssertTypedAndErasedEquality(overImpl, otherImpl, equal: false);
        AssertTypedAndErasedEquality(otherImpl, overImpl, equal: false);

        AsyncKafkaFuture<int> otherType = new AsyncKafkaFutureImpl<int>();
        Assert.False(((object)overImpl).Equals(otherType));
        Assert.False(((object)(AsyncKafkaFuture)overImpl).Equals(otherType));
        Assert.False(((object)otherType).Equals((AsyncKafkaFuture)overImpl));
    }

    [Fact]
    public void Equality_EveryNullFuture_EqualsEveryOther_TypedOrPlain()
    {
        AsyncKafkaFuture?[] nulls =
        {
            default(AsyncKafkaFuture<int>), default(AsyncKafkaFuture), default(AsyncKafkaFuture<string>),
        };
        foreach (AsyncKafkaFuture? left in nulls)
        {
            foreach (AsyncKafkaFuture? right in nulls)
            {
                Assert.True(left == right);
                Assert.True(Equals(left, right));
            }
        }
    }

    [Fact]
    public void Equality_ALiveFuture_IsUnequalToAFutureOfAnotherType_AndEqualToItsOwnErasedForm()
    {
        AsyncKafkaFuture<string> otherType = new AsyncKafkaFutureImpl<string>();
        AsyncKafkaFuture<int>[] live =
        {
            new AsyncKafkaFutureImpl<int>(),
            new AsyncKafkaFutureImpl<int>(mayCancel: false),
        };
        foreach (AsyncKafkaFuture<int> future in live)
        {
            Assert.False(future.Equals((object?)default(AsyncKafkaFuture<string>)));
            AssertUnequalBothWays(future, otherType);
            Assert.False(future.Equals(default(AsyncKafkaFuture)));
            object erased = (AsyncKafkaFuture)future;
            Assert.True(((object)future).Equals(erased));
            Assert.True(erased.Equals(future));
        }
    }

    [Theory]
    [InlineData("GetNow")]
    [InlineData("IsCancelled")]
    [InlineData("IsCompletedExceptionally")]
    public void IsDone_OverADoneValueSource_RunsNothing_UntilASettlingReadRunsEachListenerOnce(string read)
    {
        var source = new FakeFutureValueSource<int>();
        AsyncKafkaFuture<int> future = new AsyncKafkaFutureImpl<int>(source: source);
        var ran = new ConcurrentQueue<(string Listener, int Thread)>();
        AsyncKafkaFuture<int> applied = future.ThenApply(value =>
        {
            ran.Enqueue(("ThenApply", Environment.CurrentManagedThreadId));
            return value + 1;
        });
        future.WhenComplete((value, error) => ran.Enqueue(("WhenComplete", Environment.CurrentManagedThreadId)));
        source.Finish(42);

        Assert.True(future.IsDone());
        Assert.True(future.IsDone());
        future.WhenComplete((value, error) => ran.Enqueue(("WhenCompleteAfterIsDone", Environment.CurrentManagedThreadId)));
        Assert.Empty(ran);
        Assert.Equal(0, source.Reads);
        Assert.False(applied.IsDone());

        object? answer = OnNewThread(
            () => read switch
            {
                "GetNow" => future.GetNow(-1),
                "IsCancelled" => future.IsCancelled(),
                _ => future.IsCompletedExceptionally(),
            },
            out int reader);

        object expected = read == "GetNow" ? 42 : false;
        Assert.Equal(expected, answer);
        Assert.NotEqual(Environment.CurrentManagedThreadId, reader);
        Assert.Equal(
            new[] { "ThenApply", "WhenComplete", "WhenCompleteAfterIsDone" },
            ran.Select(entry => entry.Listener).OrderBy(name => name, StringComparer.Ordinal));
        Assert.All(ran, entry => Assert.Equal(reader, entry.Thread));
        Assert.Equal(1, source.Reads);

        Assert.Equal(42, future.GetNow(-1));
        Assert.False(future.IsCancelled());
        Assert.False(future.IsCompletedExceptionally());
        Assert.True(future.IsDone());
        Assert.Equal(43, applied.GetNow(-1));
        Assert.Equal(3, ran.Count);
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public void IsDone_OverAPendingValueSource_IsFalse_AndRunsNothing()
    {
        var source = new FakeFutureValueSource<int>();
        AsyncKafkaFuture<int> future = new AsyncKafkaFutureImpl<int>(source: source);
        int ran = 0;
        future.WhenComplete((value, error) => Interlocked.Increment(ref ran));

        Assert.False(future.IsDone());
        Assert.Equal(0, source.Reads);
        Assert.Equal(-1, future.GetNow(-1));
        Assert.False(future.IsDone());
        Assert.Equal(0, Volatile.Read(ref ran));
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public void Cancel_OverAnImpl_AnswersTrueAgainOnceCancelled_AsJavaDoes()
    {
        AsyncKafkaFuture<int> future = new AsyncKafkaFutureImpl<int>();

        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        Assert.True(future.Cancel(true));
        Assert.True(future.IsCancelled());
        Assert.Throws<OperationCanceledException>(() => future.GetNow(-1));
    }

    [Fact]
    public void Shape_IsAPublicAbstractClass_DerivingFromAsyncKafkaFuture_WithJavasKafkaFutureMembers()
    {
        Type open = typeof(AsyncKafkaFuture<>);
        Type type = typeof(AsyncKafkaFuture<RecordMetadata>);

        Assert.True(open.IsClass);
        Assert.True(open.IsAbstract);
        Assert.False(open.IsSealed);
        Assert.True(open.IsPublic);
        Assert.Equal(typeof(AsyncKafkaFuture), type.BaseType);
        Assert.True(typeof(AsyncKafkaFuture).IsAssignableFrom(typeof(AsyncKafkaFutureImpl<RecordMetadata>)));

        Type typeParameter = Assert.Single(open.GetGenericArguments());
        Assert.Equal(GenericParameterAttributes.None, typeParameter.GenericParameterAttributes);
        Assert.Empty(typeParameter.GetGenericParameterConstraints());

        Assert.Equal(
            new[]
            {
                "System.IEquatable`1[Confluent.Kafka.Common.AsyncKafkaFuture]",
                "System.IEquatable`1[Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata]]",
            },
            type.GetInterfaces().Select(face => face.ToString()).OrderBy(name => name, StringComparer.Ordinal));

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata] WhenComplete("
                    + "Confluent.Kafka.Common.KafkaFuture+BiConsumer`2[Confluent.Kafka.Producer.RecordMetadata,System.Exception])",
                "Confluent.Kafka.Common.AsyncKafkaFuture`1[TR] ThenApply("
                    + "Confluent.Kafka.Common.KafkaFuture+BaseFunction`2[Confluent.Kafka.Producer.RecordMetadata,TR])",
                "Confluent.Kafka.Producer.RecordMetadata GetNow(Confluent.Kafka.Producer.RecordMetadata)",
                "System.Boolean Equals(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean Equals(System.Object)",
                "System.Int32 GetHashCode()",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] Get()",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] Get(System.Threading.CancellationToken)",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] Get(System.TimeSpan)",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] ToCompletionStage()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));
        Assert.All(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.Name is not ("Equals" or "GetHashCode")),
            method => Assert.True(method.IsAbstract, method.Name + " should be abstract"));

        foreach (string inherited in new[] { "Cancel", "IsCancelled", "IsCompletedExceptionally", "IsDone", "ToString" })
        {
            MethodInfo method = type.GetMethods(BindingFlags.Public | BindingFlags.Instance).Single(m => m.Name == inherited);
            Assert.Equal(typeof(AsyncKafkaFuture), method.DeclaringType);
            Assert.True(method.IsAbstract, inherited + " should be abstract");
        }

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));

        Assert.Equal(
            new[]
            {
                "System.Boolean op_Equality(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata], "
                    + "Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean op_Inequality(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata], "
                    + "Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        ConstructorInfo constructor = Assert.Single(type.GetConstructors(AnyInstance));
        Assert.True(constructor.IsFamilyAndAssembly);
        Assert.Empty(constructor.GetParameters());

        Assert.Empty(type.GetFields(AnyInstance | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        Assert.Empty(type.GetNestedTypes(BindingFlags.Public));
    }

    private static object? OnNewThread(Func<object?> body, out int threadId)
    {
        object? result = null;
        Exception? error = null;
        int id = 0;
        Thread thread = new Thread(() =>
        {
            id = Environment.CurrentManagedThreadId;
            try
            {
                result = body();
            }
            catch (Exception e)
            {
                error = e;
            }
        });
        thread.IsBackground = true;
        thread.Start();
        Assert.True(thread.Join(BlockingCall.Deadline), $"The reading thread did not finish within {BlockingCall.Deadline}.");
        Assert.Null(error);
        threadId = id;
        return result;
    }

    private static void AssertTypedAndErasedEquality(
        AsyncKafkaFuture<RecordMetadata> typed, AsyncKafkaFuture erased, bool equal)
    {
        object boxedTyped = typed;
        object boxedErased = erased;
        Assert.Equal(equal, erased.Equals(typed));
        Assert.Equal(equal, typed.Equals(erased));
        Assert.Equal(equal, erased.Equals(boxedTyped));
        Assert.Equal(equal, typed.Equals(boxedErased));
        Assert.Equal(equal, boxedErased.Equals(typed));
        Assert.Equal(equal, boxedTyped.Equals(erased));
        Assert.Equal(equal, erased == typed);
        Assert.Equal(equal, typed == erased);
        Assert.Equal(!equal, erased != typed);
        Assert.Equal(!equal, typed != erased);
        if (equal)
        {
            Assert.Equal(typed.GetHashCode(), erased.GetHashCode());
        }
    }

    private static void AssertUnequalBothWays(object left, object right)
    {
        Assert.False(left.Equals(right), left.GetType() + " should not equal " + right.GetType());
        Assert.False(right.Equals(left), right.GetType() + " should not equal " + left.GetType());
    }

    private static void AssertFailsWith(InvalidOperationException failure, Action call)
    {
        InvalidOperationException thrown = FutureAssert.Throws<InvalidOperationException>(call);
        Assert.Same(failure, thrown);
        Assert.Equal("delivery failed", thrown.Message);
    }

    private static async Task AssertTimesOut(Task wait)
    {
        FutureTimeoutException thrown = await Assert.ThrowsAsync<FutureTimeoutException>(
            () => TestTimeout.Run(() => wait, BlockingCall.Deadline));
        Assert.Equal(string.Empty, thrown.Message);
    }

    private static async Task AssertCanceledBy(CancellationToken token, Task wait)
    {
        OperationCanceledException thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => wait, BlockingCall.Deadline));
        Assert.Equal(token, thrown.CancellationToken);
    }

    private static string[] Signatures(Type type, BindingFlags flags) =>
        type.GetMethods(flags).Select(Signature).OrderBy(signature => signature, StringComparer.Ordinal).ToArray();

    private static async Task<RecordMetadata> AwaitGetWithoutContext(AsyncKafkaFuture<RecordMetadata> future) =>
        await future.Get().ConfigureAwait(false);

    private static AsyncKafkaFutureImpl<RecordMetadata> DeliveryIn(string state)
    {
        AsyncKafkaFutureImpl<RecordMetadata> completion = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        switch (state)
        {
            case "pending":
                break;
            case "completed":
                Assert.True(completion.Complete(NewMetadata()));
                break;
            case "faulted":
                Assert.True(completion.CompleteWithError(new InvalidOperationException("delivery failed")));
                break;
            case "canceled":
                Assert.True(completion.CompleteWithError(new OperationCanceledException("The delivery was canceled.")));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, "unknown future state");
        }

        return completion;
    }

    private static AsyncKafkaFutureImpl<RecordMetadata> Delivered(RecordMetadata metadata)
    {
        AsyncKafkaFutureImpl<RecordMetadata> completion = new AsyncKafkaFutureImpl<RecordMetadata>(mayCancel: false);
        Assert.True(completion.Complete(metadata));
        return completion;
    }

    private static RecordMetadata NewMetadata() =>
        new RecordMetadata(new TopicPartition("future-topic", 3), 42L, 0, 1_700_000_000_000L, -1, -1);

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
