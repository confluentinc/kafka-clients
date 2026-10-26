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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public non-generic <see cref="AsyncKafkaFuture"/>, Java's <c>KafkaFuture&lt;Void&gt;</c>, over a
/// <see cref="KafkaFutureImpl"/>: the default-value guard, the same-instance contract of <c>Get()</c>, identity
/// equality, the erasure from <see cref="AsyncKafkaFuture{T}"/>, the static factories, and the member set.
/// </summary>
public sealed class PublicNonGenericAsyncKafkaFutureTests
{
    private const string DefaultValueMessage = "This AsyncKafkaFuture is a default value and is bound to no operation.";

    [Fact]
    public void Default_Get_ThrowsInvalidOperationException_Synchronously_WithItsMessage()
    {
        AssertGetThrowsSynchronously(default(AsyncKafkaFuture));
        AssertGetThrowsSynchronously(new AsyncKafkaFuture());

        InvalidOperationException typed =
            Assert.Throws<InvalidOperationException>(() => { _ = default(AsyncKafkaFuture<RecordMetadata>).Get(); });
        Assert.Equal(DefaultValueMessage, typed.Message);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void Get_ReturnsTheSameTaskOnEveryCall(string state)
    {
        KafkaFutureImpl<bool> impl = ImplIn(state);
        AsyncKafkaFuture future = new AsyncKafkaFuture(impl);

        Task first = future.Get();
        Task second = future.Get();

        Assert.Same(impl.GetAsync(), first);
        Assert.Same(first, second);

        _ = first.Exception;
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheImpl()
    {
        KafkaFutureImpl<bool> impl = ImplIn("completed");
        AsyncKafkaFuture a = new AsyncKafkaFuture(impl);
        AsyncKafkaFuture sameImpl = new AsyncKafkaFuture(impl);
        Assert.True(a.Equals(sameImpl));
        Assert.True(a == sameImpl);
        Assert.False(a != sameImpl);
        Assert.Equal(a.GetHashCode(), sameImpl.GetHashCode());

        KafkaFutureImpl<bool> firstImpl = ImplIn("completed");
        KafkaFutureImpl<bool> secondImpl = ImplIn("completed");
        Assert.NotSame(firstImpl, secondImpl);
        AsyncKafkaFuture x = new AsyncKafkaFuture(firstImpl);
        AsyncKafkaFuture y = new AsyncKafkaFuture(secondImpl);
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        AsyncKafkaFuture d1 = default(AsyncKafkaFuture);
        AsyncKafkaFuture d2 = new AsyncKafkaFuture();
        Assert.True(d1.Equals(d2));
        Assert.True(d1 == d2);
        Assert.False(d1 != d2);
        Assert.Equal(d1.GetHashCode(), d2.GetHashCode());
        Assert.False(d1.Equals(a));
        Assert.False(a.Equals(d1));
        Assert.True(d1 != a);
        Assert.True(a != d1);
        Assert.False(d1 == a);

        KafkaFutureImpl<bool> typedImpl = ImplIn("completed");
        object boxedEqual = sameImpl;
        object boxedDifferent = y;
        object rawImpl = impl;
        object typedOverTheSameImpl = new AsyncKafkaFuture<bool>(typedImpl);
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawImpl));
        Assert.True(new AsyncKafkaFuture(typedImpl).Equals(typedOverTheSameImpl));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public async Task Erasure_WrapsTheSameImpl_AndCompletesExactlyWhenTheTypedFutureDoes()
    {
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        AsyncKafkaFuture<bool> typed = new AsyncKafkaFuture<bool>(completion);

        AsyncKafkaFuture first = typed.Erase();
        AsyncKafkaFuture second = typed.Erase();

        Assert.Same(typed.Get(), first.Get());
        Assert.True(first == second);
        Assert.True(first == new AsyncKafkaFuture(completion));
        Assert.Equal(typed.GetHashCode(), first.GetHashCode());
        Assert.True(first != new AsyncKafkaFuture<bool>(ImplIn("completed")).Erase());

        Assert.False(first.Get().IsCompleted);
        Assert.True(completion.Complete(true));
        await TestTimeout.Run(() => first.Get(), BlockingCall.Deadline);
        Assert.True(first.Get().IsCompleted);
        Assert.True(typed.Get().IsCompleted);

        Assert.True(default(AsyncKafkaFuture<bool>).Erase() == default(AsyncKafkaFuture));
    }

    [Fact]
    public void Default_EveryMemberButEqualityAndToString_ThrowsInvalidOperationException()
    {
        AsyncKafkaFuture future = default(AsyncKafkaFuture);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        AssertDefaultThrows(() => future.Get(TimeSpan.Zero));
        AssertDefaultThrows(() => future.Get(cancellation.Token));
        AssertDefaultThrows(() => future.ToCompletionStage());
        AssertDefaultThrows(() =>
        {
            future.GetNow();
            return null;
        });
        AssertDefaultThrows(() => future.IsDone);
        AssertDefaultThrows(() => future.IsCancelled);
        AssertDefaultThrows(() => future.IsCompletedExceptionally);
        AssertDefaultThrows(() => future.Cancel(true));
        AssertDefaultThrows(() => future.ThenApply(() => 1));
        AssertDefaultThrows(() => future.WhenComplete(error => { }));
        Assert.Equal(typeof(AsyncKafkaFuture).ToString(), future.ToString());
    }

    [Fact]
    public void ThenApplyAndWhenComplete_RejectANullDelegate()
    {
        AsyncKafkaFuture[] futures =
        {
            default(AsyncKafkaFuture),
            new AsyncKafkaFuture(new KafkaFutureImpl<object?>()),
        };
        foreach (AsyncKafkaFuture future in futures)
        {
            Assert.Equal("function", Assert.Throws<ArgumentNullException>(() => future.ThenApply<int>(null!)).ParamName);
            Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => future.WhenComplete(null!)).ParamName);
        }
    }

    [Theory]
    [InlineData("succeeded")]
    [InlineData("failed")]
    public void OnceDone_ThenApplyWhenCompleteAndAllOf_RunInsideTheCall(string outcome)
    {
        InvalidOperationException failure = new InvalidOperationException("the operation failed");
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        AsyncKafkaFuture future = new AsyncKafkaFuture(completion);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Assert.True(outcome == "succeeded" ? completion.Complete(true) : completion.CompleteWithError(failure));
        cancellation.Cancel();
        int caller = Environment.CurrentManagedThreadId;
        int appliedOn = 0;
        int completedOn = 0;
        Exception? seenError = null;

        AsyncKafkaFuture<int> applied = future.ThenApply(() =>
        {
            appliedOn = Environment.CurrentManagedThreadId;
            return 1;
        });
        AsyncKafkaFuture completed = future.WhenComplete(error =>
        {
            completedOn = Environment.CurrentManagedThreadId;
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
            Assert.Equal(1, applied.GetNow(-1));
            Assert.Null(seenError);
            completed.GetNow();
            all.GetNow();
            Assert.False(completed.IsCompletedExceptionally);
            Assert.False(all.IsCompletedExceptionally);
        }
        else
        {
            Assert.Equal(0, appliedOn);
            AssertFailsWith(failure, () => applied.GetNow(-1));
            Assert.Same(failure, seenError);
            AssertFailsWith(failure, () => completed.GetNow());
            AssertFailsWith(failure, () => all.GetNow());
            _ = future.Get().Exception;
        }
    }

    [Fact]
    public async Task OverAPendingImpl_TheTimedGet_AnswersFromTheTask()
    {
        InvalidOperationException failure = new InvalidOperationException("the operation failed");
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        AsyncKafkaFuture future = new AsyncKafkaFuture(completion);

        Assert.Same(future.Get(), future.Get(Timeout.InfiniteTimeSpan));
        await AssertTimesOut(future.Get(TimeSpan.Zero));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(-5)));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(50)));

        Task waiting = future.Get(BlockingCall.Deadline);
        Assert.False(waiting.IsCompleted);
        Assert.True(completion.Complete(true));
        await TestTimeout.Run(() => waiting, BlockingCall.Deadline);
        Assert.True(waiting.Status == TaskStatus.RanToCompletion);

        KafkaFutureImpl<bool> failing = new KafkaFutureImpl<bool>();
        Task waitingForFailure = new AsyncKafkaFuture(failing).Get(BlockingCall.Deadline);
        Assert.False(waitingForFailure.IsCompleted);
        Assert.True(failing.CompleteWithError(failure));
        InvalidOperationException thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => waitingForFailure, BlockingCall.Deadline));
        Assert.Same(failure, thrown);
        Assert.Equal("the operation failed", thrown.Message);
    }

    [Fact]
    public async Task OverAPendingImpl_TheCancellableGet_AnswersFromTheTask()
    {
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        AsyncKafkaFuture future = new AsyncKafkaFuture(completion);
        using CancellationTokenSource preCanceled = new CancellationTokenSource();
        preCanceled.Cancel();
        using CancellationTokenSource firing = new CancellationTokenSource();
        using CancellationTokenSource unfired = new CancellationTokenSource();

        Assert.Same(future.Get(), future.Get(CancellationToken.None));
        Task canceledAtOnce = future.Get(preCanceled.Token);
        Assert.True(canceledAtOnce.IsCanceled);
        await AssertCanceledBy(preCanceled.Token, canceledAtOnce);

        Task canceledLater = future.Get(firing.Token);
        Assert.False(canceledLater.IsCompleted);
        firing.Cancel();
        await AssertCanceledBy(firing.Token, canceledLater);
        Assert.False(completion.IsDone);

        Task waiting = future.Get(unfired.Token);
        Assert.False(waiting.IsCompleted);
        Assert.True(completion.Complete(true));
        await TestTimeout.Run(() => waiting, BlockingCall.Deadline);
        Assert.True(waiting.Status == TaskStatus.RanToCompletion);
    }

    [Fact]
    public async Task OverAnImpl_ThenApplyAndWhenComplete_FollowTheFuture()
    {
        KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
        AsyncKafkaFuture future = new AsyncKafkaFuture(impl);
        int calls = 0;
        AsyncKafkaFuture<int> applied = future.ThenApply(() => Interlocked.Increment(ref calls) + 41);
        Exception? seen = new InvalidOperationException("The action did not run.");
        AsyncKafkaFuture completed = future.WhenComplete(error => seen = error);
        Assert.False(applied.IsDone);
        Assert.False(completed.IsDone);

        Assert.True(impl.Complete(null));
        Assert.Equal(42, await TestTimeout.Run(() => applied.Get(), BlockingCall.Deadline));
        await TestTimeout.Run(() => completed.Get(), BlockingCall.Deadline);
        Assert.Null(seen);
        Assert.Equal(1, calls);

        KafkaFutureImpl<object?> failing = new KafkaFutureImpl<object?>();
        AsyncKafkaFuture failingFuture = new AsyncKafkaFuture(failing);
        bool ran = false;
        AsyncKafkaFuture<int> skipped = failingFuture.ThenApply(() =>
        {
            ran = true;
            return 0;
        });
        Exception? seenFailure = null;
        AsyncKafkaFuture after = failingFuture.WhenComplete(error => seenFailure = error);
        KafkaException cause = new KafkaException(36, "The operation failed.", isRetriable: false);
        Assert.True(failing.CompleteWithError(cause));
        Assert.False(ran);
        Assert.Same(cause, await Assert.ThrowsAsync<KafkaException>(() => skipped.Get()));
        Assert.Same(cause, seenFailure);
        Assert.Same(cause, await Assert.ThrowsAsync<KafkaException>(() => after.Get()));
    }

    [Fact]
    public async Task OverAnImpl_TheGetForms_CancelAndToString_FollowTheFuture()
    {
        KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
        AsyncKafkaFuture future = new AsyncKafkaFuture(impl);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        await Assert.ThrowsAsync<TimeoutException>(() => future.Get(TimeSpan.Zero));
        future.GetNow();
        Assert.Same(future.Get(), future.ToCompletionStage());
        Task waiting = future.Get(cancellation.Token);
        Assert.False(waiting.IsCompleted);
        cancellation.Cancel();
        OperationCanceledException ended = await TestTimeout.Run(
            () => Assert.ThrowsAsync<OperationCanceledException>(() => waiting),
            BlockingCall.Deadline);
        Assert.Equal(cancellation.Token, ended.CancellationToken);
        Assert.False(future.IsDone);
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());

        Assert.True(future.Cancel(true));
        Assert.True(future.IsDone);
        Assert.True(future.IsCancelled);
        Assert.True(future.IsCompletedExceptionally);
        Assert.True(future.Cancel(false));
        await Assert.ThrowsAsync<TaskCanceledException>(() => future.Get());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => future.Get(TimeSpan.FromSeconds(1)));
        OperationCanceledException cancelled = Assert.Throws<OperationCanceledException>(() => future.GetNow());
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.OperationCanceledException: " + cancelled.Message + ",done=true}",
            future.ToString());

        AsyncKafkaFuture done = AsyncKafkaFuture.CompletedFuture(5);
        await TestTimeout.Run(() => done.Get(TimeSpan.Zero), BlockingCall.Deadline);
        await TestTimeout.Run(() => done.Get(cancellation.Token), BlockingCall.Deadline);
        Assert.False(done.Cancel(true));
        Assert.False(done.IsCancelled);
    }

    [Fact]
    public async Task AllOf_TakesMixedTypes_AnArrayOfOneType_AndNoFutures()
    {
        KafkaFutureImpl<int> number = new KafkaFutureImpl<int>();
        KafkaFutureImpl<string> text = new KafkaFutureImpl<string>();
        AsyncKafkaFuture mixed = AsyncKafkaFuture.AllOf(
            new AsyncKafkaFuture<int>(number),
            new AsyncKafkaFuture<string>(text));
        Assert.False(mixed.IsDone);
        Assert.True(number.Complete(1));
        Assert.False(mixed.IsDone);
        Assert.True(text.Complete("one"));
        await TestTimeout.Run(() => mixed.Get(), BlockingCall.Deadline);
        Assert.True(mixed.IsDone);

        KafkaFutureImpl<int> first = new KafkaFutureImpl<int>();
        KafkaFutureImpl<int> second = new KafkaFutureImpl<int>();
        AsyncKafkaFuture<int>[] oneType = { new AsyncKafkaFuture<int>(first), new AsyncKafkaFuture<int>(second) };
        AsyncKafkaFuture all = AsyncKafkaFuture.AllOf(oneType);
        KafkaException cause = new KafkaException(36, "The operation failed.", isRetriable: false);
        Assert.True(first.CompleteWithError(cause));
        Assert.False(all.IsDone);
        Assert.True(second.Complete(2));
        Assert.Same(cause, await Assert.ThrowsAsync<KafkaException>(() => all.Get()));
        Assert.True(all.IsCompletedExceptionally);
        Assert.False(all.IsCancelled);

        AsyncKafkaFuture none = AsyncKafkaFuture.AllOf();
        Assert.True(none.IsDone);
        Assert.False(none.IsCompletedExceptionally);
        await TestTimeout.Run(() => none.Get(), BlockingCall.Deadline);
        Assert.True(AsyncKafkaFuture.AllOf<int>().IsDone);

        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => AsyncKafkaFuture.AllOf((AsyncKafkaFuture[])null!)).ParamName);
        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => AsyncKafkaFuture.AllOf((AsyncKafkaFuture<int>[])null!)).ParamName);
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithJavasKafkaFutureMembers()
    {
        Type type = typeof(AsyncKafkaFuture);

        Assert.True(type.IsValueType);
        Assert.True(type.IsPublic);
        Assert.False(type.IsGenericType);
        Assert.Equal("Confluent.Kafka", type.Namespace);
        Assert.Contains(
            type.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        Assert.Equal(new[] { typeof(IEquatable<AsyncKafkaFuture>) }, type.GetInterfaces());

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.AsyncKafkaFuture WhenComplete(System.Action`1[System.Exception])",
                "Confluent.Kafka.AsyncKafkaFuture`1[TR] ThenApply(System.Func`1[TR])",
                "System.Boolean Cancel(System.Boolean)",
                "System.Boolean Equals(Confluent.Kafka.AsyncKafkaFuture)",
                "System.Boolean Equals(System.Object)",
                "System.Boolean get_IsCancelled()",
                "System.Boolean get_IsCompletedExceptionally()",
                "System.Boolean get_IsDone()",
                "System.Int32 GetHashCode()",
                "System.String ToString()",
                "System.Threading.Tasks.Task Get()",
                "System.Threading.Tasks.Task Get(System.Threading.CancellationToken)",
                "System.Threading.Tasks.Task Get(System.TimeSpan)",
                "System.Threading.Tasks.Task ToCompletionStage()",
                "System.Void GetNow()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.Equal(type, type.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.AsyncKafkaFuture AllOf(Confluent.Kafka.AsyncKafkaFuture[])",
                "Confluent.Kafka.AsyncKafkaFuture AllOf(Confluent.Kafka.AsyncKafkaFuture`1[T][])",
                "Confluent.Kafka.AsyncKafkaFuture`1[TU] CompletedFuture(TU)",
                "System.Boolean op_Equality(Confluent.Kafka.AsyncKafkaFuture, Confluent.Kafka.AsyncKafkaFuture)",
                "System.Boolean op_Inequality(Confluent.Kafka.AsyncKafkaFuture, Confluent.Kafka.AsyncKafkaFuture)",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Empty(type.GetNestedTypes(BindingFlags.Public));

        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.All(constructors, constructor => Assert.True(constructor.IsAssembly));
        Assert.Equal(
            new[] { typeof(KafkaFutureImpl).ToString() },
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
            new[] { typeof(KafkaFutureImpl) + " _impl" },
            fields.Select(field => field.FieldType + " " + field.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray());
    }

    private static void AssertDefaultThrows(Func<object?> call)
    {
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => { _ = call(); });
        Assert.Equal(DefaultValueMessage, thrown.Message);
    }

    private static void AssertFailsWith(InvalidOperationException failure, Action call)
    {
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(call);
        Assert.Same(failure, thrown);
        Assert.Equal("the operation failed", thrown.Message);
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

    private static void AssertGetThrowsSynchronously(AsyncKafkaFuture future)
    {
        Task? returned = null;
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => { returned = future.Get(); });
        Assert.Equal(DefaultValueMessage, thrown.Message);
        Assert.Null(returned);
    }

    private static KafkaFutureImpl<bool> ImplIn(string state)
    {
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        switch (state)
        {
            case "pending":
                break;
            case "completed":
                Assert.True(completion.Complete(true));
                break;
            case "faulted":
                Assert.True(completion.CompleteWithError(new InvalidOperationException("the operation failed")));
                break;
            case "canceled":
                Assert.True(completion.Cancel(true));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, "unknown future state");
        }

        return completion;
    }

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
