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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="KafkaFuture{T}"/> over a <see cref="KafkaFutureImpl{T}"/> or a
/// <see cref="SyncCompletion{T}"/>: the default-value guard, the same-instance contract of <c>Get()</c>, identity
/// equality, <c>IsDone</c>, the conversion to <see cref="KafkaFuture"/>, and the member set.
/// </summary>
public sealed class PublicKafkaFutureTests
{
    private const string DefaultValueMessage =
        "This KafkaFuture is a default value and is bound to no operation; only IProducer.Send and the IAdmin result "
        + "types return a usable one.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Default_Get_ThrowsInvalidOperationException_WithItsMessage()
    {
        KafkaFuture<RecordMetadata> viaDefault = default(KafkaFuture<RecordMetadata>);
        KafkaFuture<RecordMetadata> viaNew = new KafkaFuture<RecordMetadata>();

        for (int call = 0; call < 2; call++)
        {
            AssertDefaultThrows(() => viaDefault.Get());
            AssertDefaultThrows(() => viaNew.Get());
        }

        AssertDefaultThrows(() => default(KafkaFuture<long>).Get());
    }

    [Fact]
    public void Default_IsDone_ThrowsInvalidOperationException_WithTheSameMessage()
    {
        KafkaFuture<RecordMetadata> viaDefault = default(KafkaFuture<RecordMetadata>);
        KafkaFuture<RecordMetadata> viaNew = new KafkaFuture<RecordMetadata>();

        for (int call = 0; call < 2; call++)
        {
            AssertDefaultThrows(() => viaDefault.IsDone);
            AssertDefaultThrows(() => viaNew.IsDone);
        }

        AssertDefaultThrows(() => default(KafkaFuture<long>).IsDone);
    }

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void IsDone_IsFalseUntilCompleted_ThenTrue_OnSuccessAndOnFailure(string outcome)
    {
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        KafkaFuture<RecordMetadata> future = new KafkaFuture<RecordMetadata>(completion);
        KafkaFuture<RecordMetadata> copy = future;

        Assert.False(future.IsDone);
        Assert.False(copy.IsDone);

        RecordMetadata metadata = NewMetadata();
        KafkaException cause = new KafkaException(7, "The operation failed.", isRetriable: false);
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
        Assert.False(future.IsDone);

        Assert.True(outcome == "result" ? completion.TrySetResult(metadata) : completion.TrySetException(cause));

        Assert.True(future.IsDone);
        Assert.True(copy.IsDone);
        Assert.True(waiter.Join(s_deadline), $"Get() did not return within {s_deadline}.");
        Assert.Same(outcome == "result" ? metadata : cause, observed);

        Assert.True(future.IsDone);
    }

    [Fact]
    public void Get_AfterSetResult_ReturnsTheSameInstance_OnEveryCall_FromManyThreads()
    {
        const int Threads = 8;
        const int CallsPerThread = 100;

        RecordMetadata metadata = NewMetadata();
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        Assert.True(completion.TrySetResult(metadata));
        KafkaFuture<RecordMetadata> future = new KafkaFuture<RecordMetadata>(completion);

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
                        throw new TimeoutException("The reader threads did not all start.");
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
        KafkaException cause = new KafkaException(7, Message, isRetriable: true);
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        Assert.True(completion.TrySetException(cause));
        KafkaFuture<RecordMetadata> future = new KafkaFuture<RecordMetadata>(completion);

        for (int call = 0; call < 2; call++)
        {
            KafkaException thrown = Assert.Throws<KafkaException>(() => future.Get());
            Assert.Same(cause, thrown);
            Assert.Equal(Message, thrown.Message);
            Assert.Equal(7, thrown.Code);
            Assert.True(thrown.IsRetriable);
            Assert.Null(thrown.InnerException);
        }
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheCompletion()
    {
        SyncCompletion<RecordMetadata> completion = Completed(NewMetadata());
        KafkaFuture<RecordMetadata> a = new KafkaFuture<RecordMetadata>(completion);
        KafkaFuture<RecordMetadata> sameCompletion = new KafkaFuture<RecordMetadata>(completion);
        Assert.True(a.Equals(sameCompletion));
        Assert.True(a == sameCompletion);
        Assert.False(a != sameCompletion);
        Assert.Equal(a.GetHashCode(), sameCompletion.GetHashCode());

        RecordMetadata shared = NewMetadata();
        SyncCompletion<RecordMetadata> firstCompletion = Completed(shared);
        SyncCompletion<RecordMetadata> secondCompletion = Completed(shared);
        KafkaFuture<RecordMetadata> x = new KafkaFuture<RecordMetadata>(firstCompletion);
        KafkaFuture<RecordMetadata> y = new KafkaFuture<RecordMetadata>(secondCompletion);
        Assert.Same(x.Get(), y.Get());
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        SyncCompletion<RecordMetadata> pending = new SyncCompletion<RecordMetadata>();
        KafkaFuture<RecordMetadata> before = new KafkaFuture<RecordMetadata>(pending);
        int hashBefore = before.GetHashCode();
        Assert.True(pending.TrySetResult(shared));
        KafkaFuture<RecordMetadata> after = new KafkaFuture<RecordMetadata>(pending);
        Assert.True(before == after);
        Assert.Equal(hashBefore, after.GetHashCode());

        KafkaFuture<RecordMetadata> d1 = default(KafkaFuture<RecordMetadata>);
        KafkaFuture<RecordMetadata> d2 = new KafkaFuture<RecordMetadata>();
        Assert.True(d1.Equals(d2));
        Assert.True(d1 == d2);
        Assert.False(d1 != d2);
        Assert.Equal(d1.GetHashCode(), d2.GetHashCode());
        Assert.False(d1.Equals(a));
        Assert.False(a.Equals(d1));
        Assert.True(d1 != a);
        Assert.True(a != d1);
        Assert.False(d1 == a);

        object boxedEqual = sameCompletion;
        object boxedDifferent = y;
        object rawCompletion = completion;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawCompletion));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void Default_EveryMemberButEqualityAndToString_ThrowsInvalidOperationException()
    {
        KafkaFuture<RecordMetadata> future = default(KafkaFuture<RecordMetadata>);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        AssertDefaultThrows(() => future.Get(TimeSpan.Zero));
        AssertDefaultThrows(() => future.Get(cancellation.Token));
        AssertDefaultThrows(() => future.GetNow(NewMetadata()));
        AssertDefaultThrows(() => future.IsCancelled);
        AssertDefaultThrows(() => future.IsCompletedExceptionally);
        AssertDefaultThrows(() => future.Cancel(true));
        AssertDefaultThrows(() => future.ThenApply(metadata => metadata.Offset));
        AssertDefaultThrows(() => future.WhenComplete((metadata, error) => { }));

        Assert.Equal(typeof(KafkaFuture<RecordMetadata>).ToString(), future.ToString());
        KafkaFuture erased = future;
        Assert.True(erased == default(KafkaFuture));
    }

    [Fact]
    public void ThenApplyAndWhenComplete_RejectANullDelegate()
    {
        KafkaFuture<RecordMetadata>[] futures =
        {
            default(KafkaFuture<RecordMetadata>),
            new KafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>()),
            new KafkaFuture<RecordMetadata>(new SyncCompletion<RecordMetadata>()),
        };
        foreach (KafkaFuture<RecordMetadata> future in futures)
        {
            Assert.Equal("function", Assert.Throws<ArgumentNullException>(() => future.ThenApply<long>(null!)).ParamName);
            Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => future.WhenComplete(null!)).ParamName);
        }
    }

    [Theory]
    [InlineData("succeeded")]
    [InlineData("failed")]
    public void OverACompletion_TheMembersThatNeedACompletionSignal_ThrowNotSupportedException_OnlyWhilePending(
        string outcome)
    {
        RecordMetadata metadata = NewMetadata();
        KafkaException failure = new KafkaException(7, "The operation failed.", isRetriable: false);
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        KafkaFuture<RecordMetadata> future = new KafkaFuture<RecordMetadata>(completion);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        AssertNotSupported("ThenApply", () => future.ThenApply(value => value.Offset));
        AssertNotSupported("WhenComplete", () => future.WhenComplete((value, error) => { }));
        AssertNotSupported("Get", () => future.Get(TimeSpan.FromSeconds(1)));
        AssertNotSupported("Get", () => future.Get(cancellation.Token));
        AssertNotSupported("AllOf", () => KafkaFuture.AllOf(future));
        AssertNotSupported("AllOf", () => KafkaFuture.AllOf(KafkaFuture.CompletedFuture(1), future));

        Assert.True(outcome == "succeeded" ? completion.TrySetResult(metadata) : completion.TrySetException(failure));
        cancellation.Cancel();
        int caller = Environment.CurrentManagedThreadId;
        int appliedOn = 0;
        int completedOn = 0;
        RecordMetadata? seenValue = null;
        Exception? seenError = null;

        KafkaFuture<long> applied = future.ThenApply(value =>
        {
            appliedOn = Environment.CurrentManagedThreadId;
            return value.Offset;
        });
        KafkaFuture<RecordMetadata> completed = future.WhenComplete((value, error) =>
        {
            completedOn = Environment.CurrentManagedThreadId;
            seenValue = value;
            seenError = error;
        });
        KafkaFuture all = KafkaFuture.AllOf(KafkaFuture.CompletedFuture(1), future);

        Assert.True(applied.IsDone);
        Assert.True(completed.IsDone);
        Assert.True(all.IsDone);
        Assert.Equal(caller, completedOn);
        if (outcome == "succeeded")
        {
            Assert.Equal(caller, appliedOn);
            Assert.Equal(metadata.Offset, applied.Get());
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

    [Theory]
    [InlineData("pending")]
    [InlineData("succeeded")]
    [InlineData("failed")]
    [InlineData("canceled")]
    public void OverACompletion_StateReadsFollowIt_AndCancelChangesNothing(string state)
    {
        RecordMetadata metadata = NewMetadata();
        RecordMetadata alternative = NewMetadata();
        Exception failure = state == "canceled"
            ? new OperationCanceledException("The operation was canceled by its owner.")
            : new KafkaException(7, "The operation failed.", isRetriable: false);
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        if (state == "succeeded")
        {
            Assert.True(completion.TrySetResult(metadata));
        }
        else if (state != "pending")
        {
            Assert.True(completion.TrySetException(failure));
        }

        KafkaFuture<RecordMetadata> future = new KafkaFuture<RecordMetadata>(completion);
        bool failed = state == "failed" || state == "canceled";

        Assert.False(future.Cancel(true));
        Assert.False(future.Cancel(false));
        Assert.Equal(state != "pending", future.IsDone);
        Assert.False(future.IsCancelled);
        Assert.Equal(failed, future.IsCompletedExceptionally);
        if (state == "pending")
        {
            Assert.Same(alternative, future.GetNow(alternative));
            Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());
            Assert.True(completion.TrySetResult(metadata));
            Assert.Same(metadata, future.Get());
        }
        else if (state == "succeeded")
        {
            Assert.Same(metadata, future.GetNow(alternative));
            Assert.Equal("KafkaFuture{value=" + metadata + ",exception=null,done=true}", future.ToString());
        }
        else
        {
            Assert.Same(failure, Assert.Throws(failure.GetType(), () => future.GetNow(alternative)));
            Assert.Equal(
                "KafkaFuture{value=null,exception=" + failure.GetType().FullName + ": " + failure.Message + ",done=true}",
                future.ToString());
            Assert.Same(failure, Assert.Throws(failure.GetType(), () => future.Get()));
        }
    }

    [Fact]
    public void Conversion_ToKafkaFuture_IsTheSameFuture_OverEitherSource()
    {
        KafkaFutureImpl<RecordMetadata> impl = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> typed = new KafkaFuture<RecordMetadata>(impl);
        KafkaFuture erased = typed;
        Assert.True(erased == typed.Erase());
        Assert.True(erased == new KafkaFuture(impl));
        Assert.Equal(typed.GetHashCode(), erased.GetHashCode());
        Assert.False(erased.IsDone);
        Assert.True(impl.Complete(NewMetadata()));
        Assert.True(erased.IsDone);
        erased.GetNow();

        KafkaFutureImpl<RecordMetadata> failing = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture erasedFailure = new KafkaFuture<RecordMetadata>(failing);
        KafkaException cause = new KafkaException(7, "The operation failed.", isRetriable: false);
        Assert.True(failing.CompleteWithError(cause));
        Assert.Same(cause, Assert.Throws<KafkaException>(() => erasedFailure.GetNow()));
        Assert.True(erasedFailure.IsCompletedExceptionally);

        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        KafkaFuture<RecordMetadata> overCompletion = new KafkaFuture<RecordMetadata>(completion);
        KafkaFuture erasedCompletion = overCompletion;
        Assert.True(erasedCompletion == overCompletion.Erase());
        Assert.True(erasedCompletion == new KafkaFuture(completion));
        Assert.Equal(overCompletion.GetHashCode(), erasedCompletion.GetHashCode());
    }

    [Fact]
    public void Equality_BetweenATypedFutureAndTheValueItConvertsTo_IsSymmetric_OverEitherSource()
    {
        KafkaFuture<RecordMetadata> overImpl = new KafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>());
        KafkaFuture<RecordMetadata> overCompletion = new KafkaFuture<RecordMetadata>(new SyncCompletion<RecordMetadata>());
        KafkaFuture<RecordMetadata> otherImpl = new KafkaFuture<RecordMetadata>(new KafkaFutureImpl<RecordMetadata>());
        KafkaFuture<RecordMetadata> otherCompletion =
            new KafkaFuture<RecordMetadata>(new SyncCompletion<RecordMetadata>());
        KafkaFuture<RecordMetadata> none = default;

        AssertTypedAndErasedEquality(overImpl, overImpl, equal: true);
        AssertTypedAndErasedEquality(overCompletion, overCompletion, equal: true);
        AssertTypedAndErasedEquality(none, none, equal: true);
        AssertTypedAndErasedEquality(none, default(KafkaFuture), equal: true);

        AssertTypedAndErasedEquality(overImpl, otherImpl, equal: false);
        AssertTypedAndErasedEquality(overCompletion, otherCompletion, equal: false);
        AssertTypedAndErasedEquality(overImpl, overCompletion, equal: false);
        AssertTypedAndErasedEquality(overCompletion, overImpl, equal: false);
        AssertTypedAndErasedEquality(overImpl, none, equal: false);
        AssertTypedAndErasedEquality(none, overCompletion, equal: false);

        KafkaFuture<int> otherType = new KafkaFuture<int>(new KafkaFutureImpl<int>());
        Assert.False(((object)overImpl).Equals(otherType));
        Assert.False(((object)(KafkaFuture)overImpl).Equals(otherType));
        Assert.False(((object)otherType).Equals((KafkaFuture)overImpl));
    }

    [Fact]
    public void Equality_EveryDefaultFuture_EqualsEveryOther_TypedOrUntyped_Transitively_WithHashCodeZero()
    {
        object[] defaults = { default(KafkaFuture<int>), default(KafkaFuture), default(KafkaFuture<string>) };
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
        KafkaFuture<string> otherType = new KafkaFuture<string>(new KafkaFutureImpl<string>());
        KafkaFuture<int>[] live =
        {
            new KafkaFuture<int>(new KafkaFutureImpl<int>()),
            new KafkaFuture<int>(new SyncCompletion<int>()),
        };
        foreach (KafkaFuture<int> future in live)
        {
            AssertUnequalBothWays(future, default(KafkaFuture<string>));
            AssertUnequalBothWays(future, otherType);
            AssertUnequalBothWays(future, default(KafkaFuture));
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
        KafkaFuture<int> future = new KafkaFuture<int>(new KafkaFutureImpl<int>(source: source));
        var ran = new ConcurrentQueue<(string Listener, int Thread)>();
        KafkaFuture<int> applied = future.ThenApply(value =>
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
        KafkaFuture<int> future = new KafkaFuture<int>(new KafkaFutureImpl<int>(source: source));
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
        KafkaFuture<int> future = new KafkaFuture<int>(new KafkaFutureImpl<int>());

        Assert.True(future.Cancel(true));
        Assert.True(future.Cancel(false));
        Assert.True(future.Cancel(true));
        Assert.True(future.IsCancelled);
        Assert.Throws<OperationCanceledException>(() => future.GetNow(-1));
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithJavasKafkaFutureMembers()
    {
        Type open = typeof(KafkaFuture<>);
        Type type = typeof(KafkaFuture<RecordMetadata>);

        Assert.True(type.IsValueType);
        Assert.True(open.IsPublic);
        Assert.Equal("Confluent.Kafka", open.Namespace);

        Assert.Contains(
            open.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        Type typeParameter = Assert.Single(open.GetGenericArguments());
        Assert.Equal(GenericParameterAttributes.None, typeParameter.GenericParameterAttributes);
        Assert.Empty(typeParameter.GetGenericParameterConstraints());

        Assert.Equal(new[] { typeof(IEquatable<KafkaFuture<RecordMetadata>>) }, type.GetInterfaces());

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata] WhenComplete("
                    + "Confluent.Kafka.KafkaFuture+BiConsumer`2[Confluent.Kafka.RecordMetadata,System.Exception])",
                "Confluent.Kafka.KafkaFuture`1[TR] ThenApply("
                    + "Confluent.Kafka.KafkaFuture+BaseFunction`2[Confluent.Kafka.RecordMetadata,TR])",
                "Confluent.Kafka.RecordMetadata Get()",
                "Confluent.Kafka.RecordMetadata Get(System.Threading.CancellationToken)",
                "Confluent.Kafka.RecordMetadata Get(System.TimeSpan)",
                "Confluent.Kafka.RecordMetadata GetNow(Confluent.Kafka.RecordMetadata)",
                "System.Boolean Cancel(System.Boolean)",
                "System.Boolean Equals(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean Equals(System.Object)",
                "System.Boolean get_IsCancelled()",
                "System.Boolean get_IsCompletedExceptionally()",
                "System.Boolean get_IsDone()",
                "System.Int32 GetHashCode()",
                "System.String ToString()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("ToCompletionStage", AnyInstance));
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.DoesNotContain(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static),
            method => typeof(Task).IsAssignableFrom(method.ReturnType));
        Assert.Equal(type, type.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.KafkaFuture op_Implicit(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean op_Equality(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata], "
                    + "Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean op_Inequality(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata], "
                    + "Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.All(constructors, constructor => Assert.True(constructor.IsAssembly));
        Assert.Equal(
            new[] { typeof(KafkaFutureImpl<RecordMetadata>).ToString(), typeof(SyncFutureSource<RecordMetadata>).ToString() },
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
            Assert.True(property.CanRead);
            Assert.False(property.CanWrite);
        });
        FieldInfo[] fields = type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.All(fields, field =>
        {
            Assert.True(field.IsPrivate);
            Assert.True(field.IsInitOnly);
        });
        Assert.Equal(
            new[]
            {
                typeof(KafkaFutureImpl<RecordMetadata>) + " _impl",
                typeof(SyncFutureSource<RecordMetadata>) + " _source",
            },
            fields.Select(field => field.FieldType + " " + field.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray());
        Assert.Empty(type.GetNestedTypes(BindingFlags.Public));
    }

    private static void AssertDefaultThrows(Func<object?> get)
    {
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(get);
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

    private static void AssertNotSupported(string member, Func<object?> call)
    {
        NotSupportedException thrown = Assert.Throws<NotSupportedException>(call);
        Assert.Equal(
            "KafkaFuture." + member + " is not supported on a pending future returned by IProducer.Send or an IAdmin "
            + "result.",
            thrown.Message);
    }

    private static void AssertFailsWith(KafkaException failure, Action call)
    {
        KafkaException thrown = Assert.Throws<KafkaException>(call);
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

    private static SyncCompletion<RecordMetadata> Completed(RecordMetadata metadata)
    {
        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        Assert.True(completion.TrySetResult(metadata));
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

    private static RecordMetadata NewMetadata() => new RecordMetadata("future-topic", 3, 42L, 1_700_000_000_000L);

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
