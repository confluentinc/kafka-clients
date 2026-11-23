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

using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="KafkaFuture{T}"/>, which a <see cref="KafkaFutureImpl{T}"/> is: a <c>default</c> one is
/// <see langword="null"/>, the same-instance contract of <c>Get()</c>, identity equality, <c>IsDone()</c>, the typed
/// future as its own <see cref="KafkaFuture"/>, and the member set.
/// </summary>
public sealed class PublicKafkaFutureTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Default_IsNull()
    {
        KafkaFuture<RecordMetadata>? declared = default;

        Assert.Null(declared);
        Assert.Null(default(KafkaFuture<RecordMetadata>));
        Assert.Null(default(KafkaFuture<long>));
        KafkaFuture<int>?[] slots = new KafkaFuture<int>?[1];
        Assert.Null(slots[0]);
    }

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void IsDone_IsFalseUntilCompleted_ThenTrue_OnSuccessAndOnFailure(string outcome)
    {
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> future = completion;
        KafkaFuture<RecordMetadata> copy = future;

        Assert.False(future.IsDone());
        Assert.False(copy.IsDone());

        RecordMetadata metadata = NewMetadata();
        KafkaException cause = new Common.Errors.TimeoutException("The operation failed.");
        object? observed = null;
        Thread waiter = StartThread(() =>
        {
            try
            {
                observed = future.Get();
            }
            catch (Exception exception)
            {
                observed = exception;
            }
        });
        Assert.False(waiter.Join(TimeSpan.FromMilliseconds(100)), "Get() returned before the latch completed.");
        Assert.False(future.IsDone());

        Assert.True(outcome == "result" ? completion.Complete(metadata) : completion.CompleteWithError(cause));

        Assert.True(future.IsDone());
        Assert.True(copy.IsDone());
        Assert.True(waiter.Join(s_deadline), $"Get() did not return within {s_deadline}.");
        Assert.Same(outcome == "result" ? metadata : cause, outcome == "result" ? observed : FutureAssert.Unwrapped(observed));

        Assert.True(future.IsDone());
    }

    [Fact]
    public void Get_AfterSetResult_ReturnsTheSameInstance_OnEveryCall_FromManyThreads()
    {
        const int Threads = 8;
        const int CallsPerThread = 100;

        RecordMetadata metadata = NewMetadata();
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>();
        Assert.True(completion.Complete(metadata));
        KafkaFuture<RecordMetadata> future = completion;

        KafkaFuture<RecordMetadata> copy = future;

        int mismatches = 0;
        Exception?[] failures = new Exception?[Threads];
        using Barrier start = new Barrier(Threads);
        Thread[] threads = Enumerable.Range(0, Threads)
            .Select(index => StartThread(() =>
            {
                try
                {
                    if (!start.SignalAndWait(s_deadline))
                    {
                        throw new System.TimeoutException("The reader threads did not all start.");
                    }

                    for (int call = 0; call < CallsPerThread; call++)
                    {
                        KafkaFuture<RecordMetadata> handle = call % 2 == 0 ? future : copy;
                        if (!ReferenceEquals(metadata, handle.Get()))
                        {
                            Interlocked.Increment(ref mismatches);
                        }
                    }
                }
                catch (Exception exception)
                {
                    failures[index] = exception;
                }
            }))
            .ToArray();

        JoinAll(threads);
        Assert.All(failures, failure => Assert.Null(failure));
        Assert.Equal(0, mismatches);
        Assert.Same(metadata, future.Get());
    }

    [Fact]
    public void Get_AfterSetException_RethrowsTheSameInstance_Unwrapped()
    {
        const string Message = "Delivery failed: the broker rejected the record.";
        KafkaException cause = new Common.Errors.TimeoutException(Message);
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>();
        Assert.True(completion.CompleteWithError(cause));
        KafkaFuture<RecordMetadata> future = completion;

        for (int call = 0; call < 2; call++)
        {
            KafkaTimeoutException thrown = FutureAssert.Throws<KafkaTimeoutException>(() => future.Get());
            Assert.Same(cause, thrown);
            Assert.Equal(Message, thrown.Message);
            Assert.Equal(7, thrown.Code);
            Assert.IsAssignableFrom<Common.Errors.RetriableException>(thrown);
            Assert.Null(thrown.InnerException);
        }
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheCompletion()
    {
        KafkaFutureImpl<RecordMetadata> completion = Completed(NewMetadata());
        KafkaFuture<RecordMetadata> a = completion;
        KafkaFuture<RecordMetadata> sameCompletion = completion;
        Assert.True(a.Equals(sameCompletion));
        Assert.True(a == sameCompletion);
        Assert.False(a != sameCompletion);
        Assert.Equal(a.GetHashCode(), sameCompletion.GetHashCode());

        RecordMetadata shared = NewMetadata();
        KafkaFutureImpl<RecordMetadata> firstCompletion = Completed(shared);
        KafkaFutureImpl<RecordMetadata> secondCompletion = Completed(shared);
        KafkaFuture<RecordMetadata> x = firstCompletion;
        KafkaFuture<RecordMetadata> y = secondCompletion;
        Assert.Same(x.Get(), y.Get());
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        KafkaFutureImpl<RecordMetadata> pending = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> before = pending;
        int hashBefore = before.GetHashCode();
        Assert.True(pending.Complete(shared));
        KafkaFuture<RecordMetadata> after = pending;
        Assert.True(before == after);
        Assert.Equal(hashBefore, after.GetHashCode());

        KafkaFuture<RecordMetadata>? n1 = null;
        KafkaFuture<RecordMetadata>? n2 = default;
        Assert.True(n1 == n2);
        Assert.False(n1 != n2);
        Assert.True(Equals(n1, n2));
        Assert.False(a.Equals(n1));
        Assert.True(n1 != a);
        Assert.True(a != n1);
        Assert.False(n1 == a);
        Assert.False(a == n1);

        object boxedEqual = sameCompletion;
        object boxedDifferent = y;
        object rawCompletion = completion;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.Same(rawCompletion, a);
        Assert.True(a.Equals(rawCompletion));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void Null_EqualityOperators_AreNullSafe_TypedAndPlain()
    {
        KafkaFuture<RecordMetadata>? none = null;
        KafkaFuture? plainNone = none;
        KafkaFuture<RecordMetadata> live = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture plainLive = live;

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
        KafkaFuture<RecordMetadata>[] futures =
        {
            new KafkaFutureImpl<RecordMetadata>(),
            KafkaFuture.CompletedFuture(NewMetadata()),
        };
        foreach (KafkaFuture<RecordMetadata> future in futures)
        {
            Assert.Equal("function", Assert.Throws<ArgumentNullException>(() => future.ThenApply<long>(null!)).ParamName);
            Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => future.WhenComplete(null!)).ParamName);
        }
    }

    [Fact]
    public void ThenApplyWhenCompleteAndTheStatics_ReturnTheSyncEngine_TypedAndPlain()
    {
        KafkaFuture<int> typed = new KafkaFutureImpl<int>();
        KafkaFuture plain = typed;

        Assert.IsType<KafkaFutureImpl<int>>(typed.ThenApply(value => value + 1));
        Assert.IsType<KafkaFutureImpl<int>>(typed.WhenComplete((value, error) => { }));
        Assert.IsType<KafkaFutureImpl<string>>(plain.ThenApply(() => "applied"));
        Assert.IsType<KafkaFutureImpl<int>>(plain.WhenComplete(error => { }));
        Assert.IsType<KafkaFutureImpl<int>>(KafkaFuture.CompletedFuture(1));
        Assert.IsType<KafkaFutureImpl<object?>>(KafkaFuture.AllOf(typed));
        Assert.IsType<KafkaFutureImpl<object?>>(KafkaFuture.AllOf<int>(typed));
    }

    [Theory]
    [InlineData("succeeded")]
    [InlineData("failed")]
    public void OnceDone_ThenApplyWhenCompleteAndAllOf_RunInsideTheCall(string outcome)
    {
        RecordMetadata metadata = NewMetadata();
        KafkaException failure = new Common.Errors.TimeoutException("The operation failed.");
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> future = completion;
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Assert.True(outcome == "succeeded" ? completion.Complete(metadata) : completion.CompleteWithError(failure));
        cancellation.Cancel();
        int caller = Environment.CurrentManagedThreadId;
        int appliedOn = 0;
        int completedOn = 0;
        RecordMetadata? seenValue = null;
        Exception? seenError = null;

        KafkaFuture<long> applied = future.ThenApply(value =>
        {
            appliedOn = Environment.CurrentManagedThreadId;
            return value.Offset();
        });
        KafkaFuture<RecordMetadata> completed = future.WhenComplete((value, error) =>
        {
            completedOn = Environment.CurrentManagedThreadId;
            seenValue = value;
            seenError = error;
        });
        KafkaFuture all = KafkaFuture.AllOf(KafkaFuture.CompletedFuture(1), future);

        Assert.True(applied.IsDone());
        Assert.True(completed.IsDone());
        Assert.True(all.IsDone());
        Assert.Equal(caller, completedOn);
        if (outcome == "succeeded")
        {
            Assert.Equal(caller, appliedOn);
            Assert.Equal(metadata.Offset(), applied.Get());
            Assert.Same(metadata, seenValue);
            Assert.Null(seenError);
            Assert.Same(metadata, completed.Get());
            all.Get();
            Assert.Same(metadata, future.Get(TimeSpan.Zero));
            Assert.Same(metadata, future.Get(TimeSpan.FromSeconds(1)));
            Assert.Same(metadata, future.Get(cancellation.Token));
        }
        else
        {
            Assert.Equal(0, appliedOn);
            AssertFailsWith(failure, () => applied.Get());
            Assert.Null(seenValue);
            Assert.Same(failure, seenError);
            AssertFailsWith(failure, () => completed.Get());
            AssertFailsWith(failure, () => all.Get());
            AssertFailsWith(failure, () => future.Get(TimeSpan.Zero));
            AssertFailsWith(failure, () => future.Get(TimeSpan.FromSeconds(1)));
            AssertFailsWith(failure, () => future.Get(cancellation.Token));
        }
    }

    [Fact]
    public void ATypedFuture_IsItsOwnPlainFuture_TheSameObject()
    {
        KafkaFutureImpl<RecordMetadata> impl = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> typed = impl;
        KafkaFuture erased = typed;
        Assert.Same(typed, erased);
        Assert.True(erased == impl);
        Assert.Equal(typed.GetHashCode(), erased.GetHashCode());
        Assert.False(erased.IsDone());
        Assert.True(impl.Complete(NewMetadata()));
        Assert.True(erased.IsDone());
        erased.GetNow();

        KafkaFutureImpl<RecordMetadata> failing = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture erasedFailure = failing;
        KafkaException cause = new Common.Errors.TimeoutException("The operation failed.");
        Assert.True(failing.CompleteWithError(cause));
        Assert.Same(cause, FutureAssert.Throws<KafkaTimeoutException>(() => erasedFailure.GetNow()));
        Assert.True(erasedFailure.IsCompletedExceptionally());
    }

    [Fact]
    public void Equality_BetweenATypedFutureAndItsPlainView_IsSymmetric()
    {
        KafkaFuture<RecordMetadata> overImpl = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> otherImpl = new KafkaFutureImpl<RecordMetadata>();

        AssertTypedAndErasedEquality(overImpl, overImpl, equal: true);
        AssertTypedAndErasedEquality(overImpl, otherImpl, equal: false);
        AssertTypedAndErasedEquality(otherImpl, overImpl, equal: false);

        KafkaFuture<int> otherType = new KafkaFutureImpl<int>();
        Assert.False(((object)overImpl).Equals(otherType));
        Assert.False(((object)(KafkaFuture)overImpl).Equals(otherType));
        Assert.False(((object)otherType).Equals((KafkaFuture)overImpl));
    }

    [Fact]
    public void Equality_EveryNullFuture_EqualsEveryOther_TypedOrPlain()
    {
        KafkaFuture?[] nulls = { default(KafkaFuture<int>), default(KafkaFuture), default(KafkaFuture<string>) };
        foreach (KafkaFuture? left in nulls)
        {
            foreach (KafkaFuture? right in nulls)
            {
                Assert.True(left == right);
                Assert.True(Equals(left, right));
            }
        }
    }

    [Fact]
    public void Equality_ALiveFuture_IsUnequalToAFutureOfAnotherType_AndEqualToItsOwnErasedForm()
    {
        KafkaFuture<string> otherType = new KafkaFutureImpl<string>();
        KafkaFuture<int>[] live =
        {
            new KafkaFutureImpl<int>(),
            new KafkaFutureImpl<int>(mayCancel: false),
        };
        foreach (KafkaFuture<int> future in live)
        {
            Assert.False(future.Equals((object?)default(KafkaFuture<string>)));
            AssertUnequalBothWays(future, otherType);
            Assert.False(future.Equals(default(KafkaFuture)));
            object erased = (KafkaFuture)future;
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
        KafkaFuture<int> future = new KafkaFutureImpl<int>(source: source);
        var ran = new ConcurrentQueue<(string Listener, int Thread)>();
        KafkaFuture<int> applied = future.ThenApply(value =>
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
        KafkaFuture<int> future = new KafkaFutureImpl<int>(source: source);
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
        KafkaFuture<int> future = new KafkaFutureImpl<int>();

        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        Assert.True(future.Cancel(true));
        Assert.True(future.IsCancelled());
        Assert.Throws<OperationCanceledException>(() => future.GetNow(-1));
    }

    [Fact]
    public void Shape_IsAPublicAbstractClass_DerivingFromKafkaFuture_WithJavasKafkaFutureMembers()
    {
        Type open = typeof(KafkaFuture<>);
        Type type = typeof(KafkaFuture<RecordMetadata>);

        Assert.True(open.IsClass);
        Assert.True(open.IsAbstract);
        Assert.False(open.IsSealed);
        Assert.True(open.IsPublic);
        Assert.Equal(typeof(KafkaFuture), type.BaseType);
        Assert.True(typeof(KafkaFuture).IsAssignableFrom(typeof(KafkaFutureImpl<RecordMetadata>)));

        Type typeParameter = Assert.Single(open.GetGenericArguments());
        Assert.Equal(GenericParameterAttributes.None, typeParameter.GenericParameterAttributes);
        Assert.Empty(typeParameter.GetGenericParameterConstraints());

        Assert.Equal(
            new[]
            {
                "System.IEquatable`1[Confluent.Kafka.Common.KafkaFuture]",
                "System.IEquatable`1[Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata]]",
            },
            type.GetInterfaces().Select(face => face.ToString()).OrderBy(name => name, StringComparer.Ordinal));

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata] WhenComplete("
                    + "Confluent.Kafka.Common.KafkaFuture+BiConsumer`2[Confluent.Kafka.Producer.RecordMetadata,System.Exception])",
                "Confluent.Kafka.Common.KafkaFuture`1[TR] ThenApply("
                    + "Confluent.Kafka.Common.KafkaFuture+BaseFunction`2[Confluent.Kafka.Producer.RecordMetadata,TR])",
                "Confluent.Kafka.Producer.RecordMetadata Get()",
                "Confluent.Kafka.Producer.RecordMetadata Get(System.Threading.CancellationToken)",
                "Confluent.Kafka.Producer.RecordMetadata Get(System.TimeSpan)",
                "Confluent.Kafka.Producer.RecordMetadata GetNow(Confluent.Kafka.Producer.RecordMetadata)",
                "System.Boolean Equals(Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean Equals(System.Object)",
                "System.Int32 GetHashCode()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));
        Assert.All(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.Name is not ("Equals" or "GetHashCode")),
            method => Assert.True(method.IsAbstract, method.Name + " should be abstract"));

        foreach (string inherited in new[] { "Cancel", "IsCancelled", "IsCompletedExceptionally", "IsDone", "ToString" })
        {
            MethodInfo method = type.GetMethods(BindingFlags.Public | BindingFlags.Instance).Single(m => m.Name == inherited);
            Assert.Equal(typeof(KafkaFuture), method.DeclaringType);
            Assert.True(method.IsAbstract, inherited + " should be abstract");
        }

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("ToCompletionStage", AnyInstance));
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.DoesNotContain(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static),
            method => typeof(Task).IsAssignableFrom(method.ReturnType));

        Assert.Equal(
            new[]
            {
                "System.Boolean op_Equality(Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata], "
                    + "Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
                "System.Boolean op_Inequality(Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata], "
                    + "Confluent.Kafka.Common.KafkaFuture`1[Confluent.Kafka.Producer.RecordMetadata])",
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

    private static void AssertFailsWith(KafkaException failure, Action call)
    {
        KafkaTimeoutException thrown = FutureAssert.Throws<KafkaTimeoutException>(call);
        Assert.Same(failure, thrown);
        Assert.Equal("The operation failed.", thrown.Message);
    }

    private static void AssertTypedAndErasedEquality(KafkaFuture<RecordMetadata> typed, KafkaFuture erased, bool equal)
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

    private static string[] Signatures(Type type, BindingFlags flags) =>
        type.GetMethods(flags).Select(Signature).OrderBy(signature => signature, StringComparer.Ordinal).ToArray();

    private static KafkaFutureImpl<RecordMetadata> Completed(RecordMetadata metadata)
    {
        KafkaFutureImpl<RecordMetadata> completion = new KafkaFutureImpl<RecordMetadata>();
        Assert.True(completion.Complete(metadata));
        return completion;
    }

    private static Thread StartThread(Action body)
    {
        Thread thread = new Thread(() => body()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    private static void JoinAll(Thread[] threads)
    {
        foreach (Thread thread in threads)
        {
            Assert.True(thread.Join(s_deadline), $"A reader thread did not finish within {s_deadline}.");
        }
    }

    private static RecordMetadata NewMetadata() =>
        new RecordMetadata(new TopicPartition("future-topic", 3), 42L, 0, 1_700_000_000_000L, -1, -1);

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
