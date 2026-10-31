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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="AsyncKafkaFuture{T}"/> over a <see cref="KafkaFutureImpl{T}"/>: the default-value guard,
/// the same-instance contract of <c>Get()</c>, <c>ConfigureAwait</c> on the task, identity equality, the conversion to
/// <see cref="AsyncKafkaFuture"/>, and the member set.
/// </summary>
public sealed class PublicAsyncKafkaFutureTests
{
    private const string DefaultValueMessage = "This AsyncKafkaFuture is a default value and is bound to no operation.";

    [Fact]
    public void Default_Get_ThrowsInvalidOperationException_WithItsMessage()
    {
        AsyncKafkaFuture<RecordMetadata> viaDefault = default(AsyncKafkaFuture<RecordMetadata>);
        AsyncKafkaFuture<RecordMetadata> viaNew = new AsyncKafkaFuture<RecordMetadata>();

        AssertGetThrowsSynchronously(viaDefault);
        AssertGetThrowsSynchronously(viaNew);
    }

    [Fact]
    public async Task DefaultValueTask_AwaitedFuture_GetThrows()
    {
        ValueTask<AsyncKafkaFuture<RecordMetadata>> send = default(ValueTask<AsyncKafkaFuture<RecordMetadata>>);
        Assert.True(send.IsCompletedSuccessfully);

        AsyncKafkaFuture<RecordMetadata> future = await send;

        AssertGetThrowsSynchronously(future);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void Get_ReturnsTheSameTaskOnEveryCall(string state)
    {
        KafkaFutureImpl<RecordMetadata> delivery = DeliveryIn(state);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(delivery);

        Task<RecordMetadata> first = future.Get();
        Task<RecordMetadata> second = future.Get();

        Assert.Same(delivery.GetAsync(), first);
        Assert.Same(first, second);

        _ = first.Exception;
    }

    [Fact]
    public async Task Get_ConfigureAwait_FlowsThroughTheTask()
    {
        RecordMetadata metadata = NewMetadata();
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(completion);

        Task<RecordMetadata> awaiting = AwaitGetWithoutContext(future);
        Assert.False(awaiting.IsCompleted);
        Assert.True(completion.Complete(metadata));

        Assert.Same(metadata, await awaiting);
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheImpl()
    {
        KafkaFutureImpl<RecordMetadata> impl = Delivered(NewMetadata());
        AsyncKafkaFuture<RecordMetadata> a = new AsyncKafkaFuture<RecordMetadata>(impl);
        AsyncKafkaFuture<RecordMetadata> sameImpl = new AsyncKafkaFuture<RecordMetadata>(impl);
        Assert.True(a.Equals(sameImpl));
        Assert.True(a == sameImpl);
        Assert.False(a != sameImpl);
        Assert.Equal(a.GetHashCode(), sameImpl.GetHashCode());

        RecordMetadata shared = NewMetadata();
        KafkaFutureImpl<RecordMetadata> firstImpl = Delivered(shared);
        KafkaFutureImpl<RecordMetadata> secondImpl = Delivered(shared);
        Assert.NotSame(firstImpl, secondImpl);
        AsyncKafkaFuture<RecordMetadata> x = new AsyncKafkaFuture<RecordMetadata>(firstImpl);
        AsyncKafkaFuture<RecordMetadata> y = new AsyncKafkaFuture<RecordMetadata>(secondImpl);
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        AsyncKafkaFuture<RecordMetadata> d1 = default(AsyncKafkaFuture<RecordMetadata>);
        AsyncKafkaFuture<RecordMetadata> d2 = new AsyncKafkaFuture<RecordMetadata>();
        Assert.True(d1.Equals(d2));
        Assert.True(d1 == d2);
        Assert.False(d1 != d2);
        Assert.Equal(d1.GetHashCode(), d2.GetHashCode());
        Assert.False(d1.Equals(a));
        Assert.False(a.Equals(d1));
        Assert.True(d1 != a);
        Assert.True(a != d1);
        Assert.False(d1 == a);

        object boxedEqual = sameImpl;
        object boxedDifferent = y;
        object rawImpl = impl;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawImpl));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void Default_EveryMemberButEqualityAndToString_ThrowsInvalidOperationException()
    {
        AsyncKafkaFuture<RecordMetadata> future = default(AsyncKafkaFuture<RecordMetadata>);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        AssertDefaultThrows(() => future.Get(TimeSpan.Zero));
        AssertDefaultThrows(() => future.Get(cancellation.Token));
        AssertDefaultThrows(() => future.ToCompletionStage());
        AssertDefaultThrows(() => future.GetNow(NewMetadata()));
        AssertDefaultThrows(() => future.IsDone);
        AssertDefaultThrows(() => future.IsCancelled);
        AssertDefaultThrows(() => future.IsCompletedExceptionally);
        AssertDefaultThrows(() => future.Cancel(true));
        AssertDefaultThrows(() => future.ThenApply(metadata => 1));
        AssertDefaultThrows(() => future.WhenComplete((metadata, error) => { }));
        Assert.Equal(typeof(AsyncKafkaFuture<RecordMetadata>).ToString(), future.ToString());

        AsyncKafkaFuture erased = future;
        Assert.True(erased == default(AsyncKafkaFuture));
    }

    [Fact]
    public void ThenApplyAndWhenComplete_RejectANullDelegate()
    {
        AsyncKafkaFuture<RecordMetadata>[] futures =
        {
            default(AsyncKafkaFuture<RecordMetadata>),
            new AsyncKafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>()),
        };
        foreach (AsyncKafkaFuture<RecordMetadata> future in futures)
        {
            Assert.Equal(
                "function",
                Assert.Throws<ArgumentNullException>(() => future.ThenApply<int>(null!)).ParamName);
            Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => future.WhenComplete(null!)).ParamName);
        }
    }

    [Theory]
    [InlineData("succeeded")]
    [InlineData("failed")]
    public void OnceDone_ThenApplyWhenCompleteAndAllOf_RunInsideTheCall(string outcome)
    {
        RecordMetadata metadata = NewMetadata();
        InvalidOperationException failure = new InvalidOperationException("delivery failed");
        KafkaFutureImpl<RecordMetadata> delivery = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(delivery);
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
            return value.Offset;
        });
        AsyncKafkaFuture<RecordMetadata> completed = future.WhenComplete((value, error) =>
        {
            completedOn = Environment.CurrentManagedThreadId;
            seenValue = value;
            seenError = error;
        });
        AsyncKafkaFuture all = AsyncKafkaFuture.AllOf(AsyncKafkaFuture.CompletedFuture(1), future);

        Assert.True(applied.IsDone);
        Assert.True(completed.IsDone);
        Assert.True(all.IsDone);
        Assert.Equal(caller, completedOn);
        Assert.Same(future.Get(), future.Get(TimeSpan.Zero));
        Assert.Same(future.Get(), future.Get(TimeSpan.FromSeconds(1)));
        Assert.Same(future.Get(), future.Get(cancellation.Token));
        if (outcome == "succeeded")
        {
            Assert.Equal(caller, appliedOn);
            Assert.Equal(metadata.Offset, applied.GetNow(-1));
            Assert.Same(metadata, seenValue);
            Assert.Null(seenError);
            Assert.Same(metadata, completed.GetNow(NewMetadata()));
            all.GetNow();
            Assert.False(all.IsCompletedExceptionally);
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
        KafkaFutureImpl<RecordMetadata> delivery = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(delivery);

        Assert.Same(future.Get(), future.Get(Timeout.InfiniteTimeSpan));
        await AssertTimesOut(future.Get(TimeSpan.Zero));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(-5)));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(50)));

        Task<RecordMetadata> waiting = future.Get(BlockingCall.Deadline);
        Assert.False(waiting.IsCompleted);
        Assert.True(delivery.Complete(metadata));
        Assert.Same(metadata, await TestTimeout.Run(() => waiting, BlockingCall.Deadline));

        KafkaFutureImpl<RecordMetadata> failing = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        Task<RecordMetadata> waitingForFailure =
            new AsyncKafkaFuture<RecordMetadata>(failing).Get(BlockingCall.Deadline);
        Assert.False(waitingForFailure.IsCompleted);
        Assert.True(failing.CompleteWithError(failure));
        InvalidOperationException thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => waitingForFailure, BlockingCall.Deadline));
        Assert.Same(failure, thrown);
        Assert.Equal("delivery failed", thrown.Message);
    }

    [Fact]
    public async Task OverAPendingDelivery_TheCancellableGet_AnswersFromTheTask()
    {
        RecordMetadata metadata = NewMetadata();
        KafkaFutureImpl<RecordMetadata> delivery = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(delivery);
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
        Assert.False(delivery.IsDone);

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
        KafkaFutureImpl<RecordMetadata> delivery = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
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

        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(delivery);
        RecordMetadata absent = NewMetadata();

        Assert.False(future.Cancel(true));
        Assert.False(future.Cancel(false));
        Assert.Equal(state != "pending", future.IsDone);
        Assert.False(future.IsCancelled);
        Assert.Equal(state == "faulted" || state == "canceled", future.IsCompletedExceptionally);
        Assert.Same(delivery.GetAsync(), future.Get());
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
                Assert.Same(failure, Assert.Throws<InvalidOperationException>(() => future.GetNow(absent)));
                Assert.Equal(
                    "KafkaFuture{value=null,exception=System.InvalidOperationException: delivery failed,done=true}",
                    future.ToString());
                break;
            default:
                Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => future.GetNow(absent)));
                Assert.Equal(
                    "KafkaFuture{value=null,exception=System.OperationCanceledException: The delivery was canceled.,"
                        + "done=true}",
                    future.ToString());
                break;
        }

        Assert.False(future.IsCancelled);
        _ = future.Get().Exception;
    }

    [Fact]
    public void ToCompletionStage_IsTheTaskGetReturns()
    {
        AsyncKafkaFuture<RecordMetadata> overDelivery = new AsyncKafkaFuture<RecordMetadata>(DeliveryIn("pending"));
        Assert.Same(overDelivery.Get(), overDelivery.ToCompletionStage());

        AsyncKafkaFuture<RecordMetadata> overImpl =
            new AsyncKafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>());
        Assert.Same(overImpl.Get(), overImpl.ToCompletionStage());
    }

    [Fact]
    public async Task Conversion_ToAsyncKafkaFuture_IsTheSameFuture()
    {
        KafkaFutureImpl<RecordMetadata> impl = new KafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture<RecordMetadata> typed = new AsyncKafkaFuture<RecordMetadata>(impl);
        AsyncKafkaFuture erased = typed;
        Assert.True(erased == new AsyncKafkaFuture(impl));
        Assert.Equal(typed.GetHashCode(), erased.GetHashCode());
        Assert.False(erased.IsDone);
        Task waiting = erased.Get();
        Assert.True(impl.Complete(NewMetadata()));
        await TestTimeout.Run(() => waiting, BlockingCall.Deadline);
        Assert.True(erased.IsDone);

        KafkaFutureImpl<RecordMetadata> failing = new KafkaFutureImpl<RecordMetadata>();
        AsyncKafkaFuture erasedFailure = new AsyncKafkaFuture<RecordMetadata>(failing);
        KafkaException cause = new KafkaException(7, "The operation failed.", isRetriable: false);
        Assert.True(failing.CompleteWithError(cause));
        Assert.Same(cause, Assert.Throws<KafkaException>(() => erasedFailure.GetNow()));
        Assert.True(erasedFailure.IsCompletedExceptionally);
    }

    [Fact]
    public void Equality_BetweenATypedFutureAndTheValueItConvertsTo_IsSymmetric()
    {
        AsyncKafkaFuture<RecordMetadata> overImpl =
            new AsyncKafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>());
        AsyncKafkaFuture<RecordMetadata> otherImpl =
            new AsyncKafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>());
        AsyncKafkaFuture<RecordMetadata> none = default;

        AssertTypedAndErasedEquality(overImpl, overImpl, equal: true);
        AssertTypedAndErasedEquality(none, none, equal: true);
        AssertTypedAndErasedEquality(none, default(AsyncKafkaFuture), equal: true);

        AssertTypedAndErasedEquality(overImpl, otherImpl, equal: false);
        AssertTypedAndErasedEquality(overImpl, none, equal: false);
        AssertTypedAndErasedEquality(none, otherImpl, equal: false);

        AsyncKafkaFuture<int> otherType = new AsyncKafkaFuture<int>(new KafkaFutureImpl<int>());
        Assert.False(((object)overImpl).Equals(otherType));
        Assert.False(((object)(AsyncKafkaFuture)overImpl).Equals(otherType));
        Assert.False(((object)otherType).Equals((AsyncKafkaFuture)overImpl));
    }

    [Fact]
    public void Equality_EveryDefaultFuture_EqualsEveryOther_TypedOrUntyped_Transitively_WithHashCodeZero()
    {
        object[] defaults =
        {
            default(AsyncKafkaFuture<int>), default(AsyncKafkaFuture), default(AsyncKafkaFuture<string>),
        };
        foreach (object left in defaults)
        {
            Assert.Equal(0, left.GetHashCode());
            foreach (object right in defaults)
            {
                Assert.True(left.Equals(right), left.GetType() + " should equal " + right.GetType());
            }
        }
    }

    [Fact]
    public void Equality_ALiveFuture_IsUnequalToAFutureOfAnotherType_AndEqualToItsOwnErasedForm()
    {
        AsyncKafkaFuture<string> otherType = new AsyncKafkaFuture<string>(new KafkaFutureImpl<string>());
        AsyncKafkaFuture<int>[] live =
        {
            new AsyncKafkaFuture<int>(new KafkaFutureImpl<int>()),
            new AsyncKafkaFuture<int>(new KafkaFutureImpl<int>(mayCancel: false)),
        };
        foreach (AsyncKafkaFuture<int> future in live)
        {
            AssertUnequalBothWays(future, default(AsyncKafkaFuture<string>));
            AssertUnequalBothWays(future, otherType);
            AssertUnequalBothWays(future, default(AsyncKafkaFuture));
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
        AsyncKafkaFuture<int> future = new AsyncKafkaFuture<int>(new KafkaFutureImpl<int>(source: source));
        var ran = new ConcurrentQueue<(string Listener, int Thread)>();
        AsyncKafkaFuture<int> applied = future.ThenApply(value =>
        {
            ran.Enqueue(("ThenApply", Environment.CurrentManagedThreadId));
            return value + 1;
        });
        future.WhenComplete((value, error) => ran.Enqueue(("WhenComplete", Environment.CurrentManagedThreadId)));
        source.Finish(42);

        Assert.True(future.IsDone);
        Assert.True(future.IsDone);
        future.WhenComplete((value, error) => ran.Enqueue(("WhenCompleteAfterIsDone", Environment.CurrentManagedThreadId)));
        Assert.Empty(ran);
        Assert.Equal(0, source.Reads);
        Assert.False(applied.IsDone);

        object? answer = OnNewThread(
            () => read switch
            {
                "GetNow" => future.GetNow(-1),
                "IsCancelled" => future.IsCancelled,
                _ => future.IsCompletedExceptionally,
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
        Assert.False(future.IsCancelled);
        Assert.False(future.IsCompletedExceptionally);
        Assert.True(future.IsDone);
        Assert.Equal(43, applied.GetNow(-1));
        Assert.Equal(3, ran.Count);
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public void IsDone_OverAPendingValueSource_IsFalse_AndRunsNothing()
    {
        var source = new FakeFutureValueSource<int>();
        AsyncKafkaFuture<int> future = new AsyncKafkaFuture<int>(new KafkaFutureImpl<int>(source: source));
        int ran = 0;
        future.WhenComplete((value, error) => Interlocked.Increment(ref ran));

        Assert.False(future.IsDone);
        Assert.Equal(0, source.Reads);
        Assert.Equal(-1, future.GetNow(-1));
        Assert.False(future.IsDone);
        Assert.Equal(0, Volatile.Read(ref ran));
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public void Cancel_OverAnImpl_AnswersTrueAgainOnceCancelled_AsJavaDoes()
    {
        AsyncKafkaFuture<int> future = new AsyncKafkaFuture<int>(new KafkaFutureImpl<int>());

        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        Assert.True(future.Cancel(true));
        Assert.True(future.IsCancelled);
        Assert.Throws<OperationCanceledException>(() => future.GetNow(-1));
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithJavasKafkaFutureMembers()
    {
        Type open = typeof(AsyncKafkaFuture<>);
        Type type = typeof(AsyncKafkaFuture<RecordMetadata>);

        Assert.True(type.IsValueType);
        Assert.True(open.IsPublic);

        Assert.Contains(
            open.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        Type typeParameter = Assert.Single(open.GetGenericArguments());
        Assert.Equal(GenericParameterAttributes.None, typeParameter.GenericParameterAttributes);
        Assert.Empty(typeParameter.GetGenericParameterConstraints());

        Assert.Equal(new[] { typeof(IEquatable<AsyncKafkaFuture<RecordMetadata>>) }, type.GetInterfaces());

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata] WhenComplete("
                    + "Confluent.Kafka.Common.KafkaFuture+BiConsumer`2[Confluent.Kafka.Producer.RecordMetadata,System.Exception])",
                "Confluent.Kafka.Common.AsyncKafkaFuture`1[TR] ThenApply("
                    + "Confluent.Kafka.Common.KafkaFuture+BaseFunction`2[Confluent.Kafka.Producer.RecordMetadata,TR])",
                "Confluent.Kafka.Producer.RecordMetadata GetNow(Confluent.Kafka.Producer.RecordMetadata)",
                "System.Boolean Cancel(System.Boolean)",
                "System.Boolean Equals(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean Equals(System.Object)",
                "System.Boolean get_IsCancelled()",
                "System.Boolean get_IsCompletedExceptionally()",
                "System.Boolean get_IsDone()",
                "System.Int32 GetHashCode()",
                "System.String ToString()",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] Get()",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] Get(System.Threading.CancellationToken)",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] Get(System.TimeSpan)",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.Producer.RecordMetadata] ToCompletionStage()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.Equal(type, type.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.AsyncKafkaFuture op_Implicit(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean op_Equality(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata], "
                    + "Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean op_Inequality(Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata], "
                    + "Confluent.Kafka.Common.AsyncKafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Empty(type.GetNestedTypes(BindingFlags.Public));

        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.All(constructors, constructor => Assert.True(constructor.IsAssembly));
        Assert.Equal(
            new[] { typeof(KafkaFutureImpl<RecordMetadata>).ToString() },
            constructors
                .Select(constructor => Assert.Single(constructor.GetParameters()).ParameterType.ToString())
                .OrderBy(name => name, StringComparer.Ordinal)
                .ToArray());

        Assert.Empty(type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static);
        Assert.Equal(
            new[] { "IsCancelled", "IsCompletedExceptionally", "IsDone" },
            properties.Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray());
        Assert.All(properties, property =>
        {
            Assert.Equal(typeof(bool), property.PropertyType);
            Assert.False(property.CanWrite);
        });
        FieldInfo[] fields = type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.All(fields, field =>
        {
            Assert.True(field.IsPrivate);
            Assert.True(field.IsInitOnly);
        });
        Assert.Equal(
            new[] { typeof(KafkaFutureImpl<RecordMetadata>) + " _impl" },
            fields.Select(field => field.FieldType + " " + field.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray());
    }

    private static void AssertDefaultThrows(Func<object?> call)
    {
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => { _ = call(); });
        Assert.Equal(DefaultValueMessage, thrown.Message);
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
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(call);
        Assert.Same(failure, thrown);
        Assert.Equal("delivery failed", thrown.Message);
    }

    private static async Task AssertTimesOut(Task wait)
    {
        TimeoutException thrown = await Assert.ThrowsAsync<TimeoutException>(
            () => TestTimeout.Run(() => wait, BlockingCall.Deadline));
        Assert.Equal(new TimeoutException().Message, thrown.Message);
    }

    private static async Task AssertCanceledBy(CancellationToken token, Task wait)
    {
        OperationCanceledException thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => wait, BlockingCall.Deadline));
        Assert.Equal(token, thrown.CancellationToken);
    }

    private static string[] Signatures(Type type, BindingFlags flags) =>
        type.GetMethods(flags).Select(Signature).OrderBy(signature => signature, StringComparer.Ordinal).ToArray();

    private static void AssertGetThrowsSynchronously(AsyncKafkaFuture<RecordMetadata> future)
    {
        Task<RecordMetadata>? returned = null;
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => { returned = future.Get(); });
        Assert.Equal(DefaultValueMessage, thrown.Message);
        Assert.Null(returned);
    }

    private static async Task<RecordMetadata> AwaitGetWithoutContext(AsyncKafkaFuture<RecordMetadata> future) =>
        await future.Get().ConfigureAwait(false);

    private static KafkaFutureImpl<RecordMetadata> DeliveryIn(string state)
    {
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
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

    private static KafkaFutureImpl<RecordMetadata> Delivered(RecordMetadata metadata)
    {
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>(mayCancel: false);
        Assert.True(completion.Complete(metadata));
        return completion;
    }

    private static RecordMetadata NewMetadata() => new RecordMetadata("future-topic", 3, 42L, 1_700_000_000_000L);

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
