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
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Internal;

using Confluent.Kafka.Producer;
using Xunit;
using FutureTimeoutException = Confluent.Kafka.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public non-generic <see cref="AsyncKafkaFuture"/>, Java's <c>KafkaFuture&lt;Void&gt;</c>, which every
/// <see cref="AsyncKafkaFuture{T}"/> is: a <c>default</c> one is <see langword="null"/>, the same-instance contract of
/// <c>Get()</c>, identity equality, a typed future seen as one, the static factories and their null checks, and the
/// member set.
/// </summary>
public sealed class PublicNonGenericAsyncKafkaFutureTests
{
    [Fact]
    public void Default_IsNull()
    {
        AsyncKafkaFuture? declared = default;

        Assert.Null(declared);
        Assert.Null(default(AsyncKafkaFuture));
        Assert.Null((AsyncKafkaFuture?)default(AsyncKafkaFuture<RecordMetadata>));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void Get_ReturnsTheSameTaskOnEveryCall(string state)
    {
        AsyncKafkaFutureImpl<bool> impl = ImplIn(state);
        AsyncKafkaFuture future = impl;

        Task first = future.Get();
        Task second = future.Get();

        Assert.Same(impl.Get(), first);
        Assert.Same(first, second);
        if (state == "pending" || state == "faulted")
        {
            Assert.NotSame(impl.ToCompletionStage(), first);
        }
        else
        {
            // A value or a cancellation has nothing to wrap: the task is the raw one.
            Assert.Same(impl.ToCompletionStage(), first);
        }

        _ = first.Exception;
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheImpl()
    {
        AsyncKafkaFutureImpl<bool> impl = ImplIn("completed");
        AsyncKafkaFuture a = impl;
        AsyncKafkaFuture sameImpl = impl;
        Assert.True(a.Equals(sameImpl));
        Assert.True(a == sameImpl);
        Assert.False(a != sameImpl);
        Assert.Equal(a.GetHashCode(), sameImpl.GetHashCode());

        AsyncKafkaFutureImpl<bool> firstImpl = ImplIn("completed");
        AsyncKafkaFutureImpl<bool> secondImpl = ImplIn("completed");
        Assert.NotSame(firstImpl, secondImpl);
        AsyncKafkaFuture x = firstImpl;
        AsyncKafkaFuture y = secondImpl;
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        AsyncKafkaFuture? n1 = null;
        AsyncKafkaFuture? n2 = default;
        Assert.True(n1 == n2);
        Assert.False(n1 != n2);
        Assert.True(Equals(n1, n2));
        Assert.False(a.Equals(n1));
        Assert.True(n1 != a);
        Assert.True(a != n1);
        Assert.False(n1 == a);

        AsyncKafkaFutureImpl<bool> typedImpl = ImplIn("completed");
        object boxedEqual = sameImpl;
        object boxedDifferent = y;
        object rawImpl = impl;
        object typedOverTheSameImpl = typedImpl;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.Same(rawImpl, a);
        Assert.True(a.Equals(rawImpl));
        Assert.True(((AsyncKafkaFuture)typedImpl).Equals(typedOverTheSameImpl));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public async Task APlainView_IsTheTypedFutureItself_AndCompletesExactlyWhenTheTypedFutureDoes()
    {
        AsyncKafkaFutureImpl<bool> completion = new AsyncKafkaFutureImpl<bool>();
        AsyncKafkaFuture<bool> typed = completion;

        AsyncKafkaFuture first = typed;
        AsyncKafkaFuture second = typed;

        Assert.Same(typed, first);
        Assert.Same(typed.Get(), first.Get());
        Assert.Same(typed.ToCompletionStage(), first.ToCompletionStage());
        Assert.True(first == second);
        Assert.True(first == completion);
        Assert.Equal(typed.GetHashCode(), first.GetHashCode());
        Assert.True(first != ImplIn("completed"));

        Assert.False(first.Get().IsCompleted);
        Assert.True(completion.Complete(true));
        await TestTimeout.Run(() => first.Get(), BlockingCall.Deadline);
        Assert.True(first.Get().IsCompleted);
        Assert.True(typed.Get().IsCompleted);

        AsyncKafkaFuture? plainNull = default(AsyncKafkaFuture<bool>);
        Assert.Null(plainNull);
        Assert.True(plainNull == default(AsyncKafkaFuture));
    }

    [Fact]
    public void AllOf_ANullFuture_ThrowsArgumentNullException_NamingFutures_BothOverloads()
    {
        AsyncKafkaFutureImpl<int> live = new AsyncKafkaFutureImpl<int>();
        string expected = new ArgumentNullException("futures", "futures must not contain null").Message;

        foreach (Func<AsyncKafkaFuture> call in new Func<AsyncKafkaFuture>[]
        {
            () => AsyncKafkaFuture.AllOf(live, null!),
            () => AsyncKafkaFuture.AllOf(null!, live),
            () => AsyncKafkaFuture.AllOf(new AsyncKafkaFuture[] { null! }),
            () => AsyncKafkaFuture.AllOf<int>(live, null!),
            () => AsyncKafkaFuture.AllOf(new AsyncKafkaFuture<int>[] { live, null! }),
        })
        {
            ArgumentNullException thrown = Assert.Throws<ArgumentNullException>(() => call());
            Assert.Equal("futures", thrown.ParamName);
            Assert.Equal(expected, thrown.Message);
        }

        Assert.True(live.Complete(1));
    }

    [Fact]
    public void ThenApplyAndWhenComplete_RejectANullDelegate()
    {
        AsyncKafkaFuture[] futures =
        {
            new AsyncKafkaFutureImpl<object?>(),
            AsyncKafkaFuture.CompletedFuture(1),
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
        AsyncKafkaFutureImpl<bool> completion = new AsyncKafkaFutureImpl<bool>();
        AsyncKafkaFuture future = completion;
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
            Assert.Equal(1, applied.GetNow(-1));
            Assert.Null(seenError);
            completed.GetNow();
            all.GetNow();
            Assert.False(completed.IsCompletedExceptionally());
            Assert.False(all.IsCompletedExceptionally());
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
        AsyncKafkaFutureImpl<bool> completion = new AsyncKafkaFutureImpl<bool>();
        AsyncKafkaFuture future = completion;

        Assert.Same(future.Get(), future.Get(Timeout.InfiniteTimeSpan));
        await AssertTimesOut(future.Get(TimeSpan.Zero));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(-5)));
        await AssertTimesOut(future.Get(TimeSpan.FromMilliseconds(50)));

        Task waiting = future.Get(BlockingCall.Deadline);
        Assert.False(waiting.IsCompleted);
        Assert.True(completion.Complete(true));
        await TestTimeout.Run(() => waiting, BlockingCall.Deadline);
        Assert.True(waiting.Status == TaskStatus.RanToCompletion);

        AsyncKafkaFutureImpl<bool> failing = new AsyncKafkaFutureImpl<bool>();
        Task waitingForFailure = ((AsyncKafkaFuture)failing).Get(BlockingCall.Deadline);
        Assert.False(waitingForFailure.IsCompleted);
        Assert.True(failing.CompleteWithError(failure));
        InvalidOperationException thrown = await FutureAssert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => waitingForFailure, BlockingCall.Deadline));
        Assert.Same(failure, thrown);
        Assert.Equal("the operation failed", thrown.Message);
    }

    [Fact]
    public async Task OverAPendingImpl_TheCancellableGet_AnswersFromTheTask()
    {
        AsyncKafkaFutureImpl<bool> completion = new AsyncKafkaFutureImpl<bool>();
        AsyncKafkaFuture future = completion;
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
        Assert.False(completion.IsDone());

        Task waiting = future.Get(unfired.Token);
        Assert.False(waiting.IsCompleted);
        Assert.True(completion.Complete(true));
        await TestTimeout.Run(() => waiting, BlockingCall.Deadline);
        Assert.True(waiting.Status == TaskStatus.RanToCompletion);
    }

    [Fact]
    public async Task OverAnImpl_ThenApplyAndWhenComplete_FollowTheFuture()
    {
        AsyncKafkaFutureImpl<object?> impl = new AsyncKafkaFutureImpl<object?>();
        AsyncKafkaFuture future = impl;
        int calls = 0;
        AsyncKafkaFuture<int> applied = future.ThenApply(() => Interlocked.Increment(ref calls) + 41);
        Exception? seen = new InvalidOperationException("The action did not run.");
        AsyncKafkaFuture completed = future.WhenComplete(error => seen = error);
        Assert.False(applied.IsDone());
        Assert.False(completed.IsDone());

        Assert.True(impl.Complete(null));
        Assert.Equal(42, await TestTimeout.Run(() => applied.Get(), BlockingCall.Deadline));
        await TestTimeout.Run(() => completed.Get(), BlockingCall.Deadline);
        Assert.Null(seen);
        Assert.Equal(1, calls);

        AsyncKafkaFutureImpl<object?> failing = new AsyncKafkaFutureImpl<object?>();
        AsyncKafkaFuture failingFuture = failing;
        bool ran = false;
        AsyncKafkaFuture<int> skipped = failingFuture.ThenApply(() =>
        {
            ran = true;
            return 0;
        });
        Exception? seenFailure = null;
        AsyncKafkaFuture after = failingFuture.WhenComplete(error => seenFailure = error);
        KafkaException cause = new Common.Errors.TopicExistsException("The operation failed.");
        Assert.True(failing.CompleteWithError(cause));
        Assert.False(ran);
        Assert.Same(cause, await FutureAssert.ThrowsAsync<TopicExistsException>(() => skipped.Get()));
        Assert.Same(cause, seenFailure);
        Assert.Same(cause, await FutureAssert.ThrowsAsync<TopicExistsException>(() => after.Get()));
    }

    [Fact]
    public async Task OverAnImpl_TheGetForms_CancelAndToString_FollowTheFuture()
    {
        AsyncKafkaFutureImpl<object?> impl = new AsyncKafkaFutureImpl<object?>();
        AsyncKafkaFuture future = impl;
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        await Assert.ThrowsAsync<FutureTimeoutException>(() => future.Get(TimeSpan.Zero));
        future.GetNow();
        Assert.Same(future.ToCompletionStage(), future.ToCompletionStage());
        Assert.NotSame(future.Get(), future.ToCompletionStage());
        Task waiting = future.Get(cancellation.Token);
        Assert.False(waiting.IsCompleted);
        cancellation.Cancel();
        OperationCanceledException ended = await TestTimeout.Run(
            () => Assert.ThrowsAsync<OperationCanceledException>(() => waiting),
            BlockingCall.Deadline);
        Assert.Equal(cancellation.Token, ended.CancellationToken);
        Assert.False(future.IsDone());
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());

        Assert.True(future.Cancel(true));
        Assert.True(future.IsDone());
        Assert.True(future.IsCancelled());
        Assert.True(future.IsCompletedExceptionally());
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
        Assert.False(done.IsCancelled());
    }

    [Fact]
    public async Task AllOf_TakesMixedTypes_AnArrayOfOneType_AndNoFutures()
    {
        AsyncKafkaFutureImpl<int> number = new AsyncKafkaFutureImpl<int>();
        AsyncKafkaFutureImpl<string> text = new AsyncKafkaFutureImpl<string>();
        AsyncKafkaFuture mixed = AsyncKafkaFuture.AllOf(number, text);
        Assert.False(mixed.IsDone());
        Assert.True(number.Complete(1));
        Assert.False(mixed.IsDone());
        Assert.True(text.Complete("one"));
        await TestTimeout.Run(() => mixed.Get(), BlockingCall.Deadline);
        Assert.True(mixed.IsDone());

        AsyncKafkaFutureImpl<int> first = new AsyncKafkaFutureImpl<int>();
        AsyncKafkaFutureImpl<int> second = new AsyncKafkaFutureImpl<int>();
        AsyncKafkaFuture<int>[] oneType = { first, second };
        AsyncKafkaFuture all = AsyncKafkaFuture.AllOf(oneType);
        KafkaException cause = new Common.Errors.TopicExistsException("The operation failed.");
        Assert.True(first.CompleteWithError(cause));
        Assert.False(all.IsDone());
        Assert.True(second.Complete(2));
        Assert.Same(cause, await FutureAssert.ThrowsAsync<TopicExistsException>(() => all.Get()));
        Assert.True(all.IsCompletedExceptionally());
        Assert.False(all.IsCancelled());

        AsyncKafkaFuture none = AsyncKafkaFuture.AllOf();
        Assert.True(none.IsDone());
        Assert.False(none.IsCompletedExceptionally());
        await TestTimeout.Run(() => none.Get(), BlockingCall.Deadline);
        Assert.True(AsyncKafkaFuture.AllOf<int>().IsDone());

        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => AsyncKafkaFuture.AllOf((AsyncKafkaFuture[])null!)).ParamName);
        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => AsyncKafkaFuture.AllOf((AsyncKafkaFuture<int>[])null!)).ParamName);
    }

    [Fact]
    public void Shape_IsAPublicAbstractClass_WithJavasKafkaFutureMembers()
    {
        Type type = typeof(AsyncKafkaFuture);

        Assert.True(type.IsClass);
        Assert.True(type.IsAbstract);
        Assert.False(type.IsSealed);
        Assert.True(type.IsPublic);
        Assert.False(type.IsGenericType);
        Assert.Equal(typeof(object), type.BaseType);

        Assert.Equal(new[] { typeof(IEquatable<AsyncKafkaFuture>) }, type.GetInterfaces());

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.AsyncKafkaFuture WhenComplete(System.Action`1[System.Exception])",
                "Confluent.Kafka.Common.AsyncKafkaFuture`1[TR] ThenApply(System.Func`1[TR])",
                "System.Boolean Cancel(System.Boolean)",
                "System.Boolean Equals(Confluent.Kafka.Common.AsyncKafkaFuture)",
                "System.Boolean Equals(System.Object)",
                "System.Boolean IsCancelled()",
                "System.Boolean IsCompletedExceptionally()",
                "System.Boolean IsDone()",
                "System.Int32 GetHashCode()",
                "System.String ToString()",
                "System.Threading.Tasks.Task Get()",
                "System.Threading.Tasks.Task Get(System.Threading.CancellationToken)",
                "System.Threading.Tasks.Task Get(System.TimeSpan)",
                "System.Threading.Tasks.Task ToCompletionStage()",
                "System.Void GetNow()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));
        foreach (MethodInfo method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            bool abstractJavaMember = method.Name is not ("Get" or "ToCompletionStage" or "Equals" or "GetHashCode");
            Assert.Equal(abstractJavaMember, method.IsAbstract);
        }

        Assert.All(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.Name is "Get" or "ToCompletionStage"),
            method => Assert.False(method.IsVirtual, method + " should not be virtual"));
        MethodInfo[] hooks = type.GetMethods(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
        Assert.Equal(4, hooks.Length);
        Assert.All(hooks, hook =>
        {
            Assert.True(hook.IsFamilyAndAssembly, hook + " should be private protected");
            Assert.True(hook.IsAbstract, hook + " should be abstract");
        });

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.Equal(type, type.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.AsyncKafkaFuture AllOf(Confluent.Kafka.Common.AsyncKafkaFuture[])",
                "Confluent.Kafka.Common.AsyncKafkaFuture AllOf(Confluent.Kafka.Common.AsyncKafkaFuture`1[T][])",
                "Confluent.Kafka.Common.AsyncKafkaFuture`1[TU] CompletedFuture(TU)",
                "System.Boolean op_Equality(Confluent.Kafka.Common.AsyncKafkaFuture, Confluent.Kafka.Common.AsyncKafkaFuture)",
                "System.Boolean op_Inequality(Confluent.Kafka.Common.AsyncKafkaFuture, Confluent.Kafka.Common.AsyncKafkaFuture)",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Empty(type.GetNestedTypes(BindingFlags.Public));

        ConstructorInfo constructor = Assert.Single(type.GetConstructors(AnyInstance));
        Assert.True(constructor.IsFamilyAndAssembly);
        Assert.Empty(constructor.GetParameters());

        Assert.Empty(type.GetFields(AnyInstance | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
    }

    private static void AssertFailsWith(InvalidOperationException failure, Action call)
    {
        InvalidOperationException thrown = FutureAssert.Throws<InvalidOperationException>(call);
        Assert.Same(failure, thrown);
        Assert.Equal("the operation failed", thrown.Message);
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

    private static AsyncKafkaFutureImpl<bool> ImplIn(string state)
    {
        AsyncKafkaFutureImpl<bool> completion = new AsyncKafkaFutureImpl<bool>();
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
