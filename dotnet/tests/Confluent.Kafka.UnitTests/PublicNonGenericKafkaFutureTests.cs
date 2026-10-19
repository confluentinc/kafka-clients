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
/// Tests the public non-generic <see cref="KafkaFuture"/>, Java's <c>KafkaFuture&lt;Void&gt;</c>, over a
/// <see cref="KafkaFutureImpl"/> or a <see cref="SyncFutureSource"/>: the default-value guard, the same-instance failure
/// contract, identity equality, the erasure from <see cref="KafkaFuture{T}"/>, the static factories, and the member set.
/// </summary>
public sealed class PublicNonGenericKafkaFutureTests
{
    private const string DefaultValueMessage =
        "This KafkaFuture is a default value and is bound to no operation; only IProducer.Send and the IAdmin result "
        + "types return a usable one.";

    [Fact]
    public void Default_GetAndIsDone_ThrowInvalidOperationException_WithItsMessage()
    {
        KafkaFuture viaDefault = default(KafkaFuture);
        KafkaFuture viaNew = new KafkaFuture();

        for (int call = 0; call < 2; call++)
        {
            AssertDefaultThrows(BlockingCall.RunWait(() => viaDefault.Get()));
            AssertDefaultThrows(BlockingCall.RunWait(() => viaNew.Get()));
            AssertDefaultThrows(BlockingCall.RunGet(() => viaDefault.IsDone));
            AssertDefaultThrows(BlockingCall.RunGet(() => viaNew.IsDone));
        }

        AssertDefaultThrows(BlockingCall.RunGet(() => default(KafkaFuture<RecordMetadata>).Get()));
    }

    [Fact]
    public void Get_BlocksUntilTheSourceSucceeds_ThenReturns_OnEveryCall()
    {
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        KafkaFuture future = new KafkaFuture(completion);

        BlockingCall waiting = BlockingCall.StartWait(() => future.Get());
        waiting.AssertBlocked();

        Assert.True(completion.TrySetResult(true));

        Assert.Same(BlockingCall.Returned, waiting.Join());
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get()));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get()));
    }

    [Fact]
    public void Get_AfterFailure_RethrowsTheSameInstance_Unwrapped()
    {
        const string Message = "The operation failed: the broker rejected it.";
        KafkaException cause = new KafkaException(36, Message, isRetriable: false);
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        KafkaFuture future = new KafkaFuture(completion);

        BlockingCall waiting = BlockingCall.StartWait(() => future.Get());
        waiting.AssertBlocked();
        Assert.True(completion.TrySetException(cause));

        Assert.Same(cause, waiting.Join());
        for (int call = 0; call < 2; call++)
        {
            KafkaException thrown = Assert.IsType<KafkaException>(BlockingCall.RunWait(() => future.Get()));
            Assert.Same(cause, thrown);
            Assert.Equal(Message, thrown.Message);
            Assert.Equal(36, thrown.Code);
            Assert.Null(thrown.InnerException);
        }
    }

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void IsDone_IsFalseUntilCompleted_ThenTrue_OnSuccessAndOnFailure(string outcome)
    {
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        KafkaFuture future = new KafkaFuture(completion);

        Assert.False(future.IsDone);
        Assert.False(future.IsDone);

        Assert.True(outcome == "result"
            ? completion.TrySetResult(true)
            : completion.TrySetException(new KafkaException(-1, "The operation failed.", isRetriable: false)));

        Assert.True(future.IsDone);
        Assert.True(future.IsDone);
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheSource()
    {
        SyncCompletion<bool> source = Completed();
        KafkaFuture a = new KafkaFuture(source);
        KafkaFuture sameSource = new KafkaFuture(source);
        Assert.True(a.Equals(sameSource));
        Assert.True(a == sameSource);
        Assert.False(a != sameSource);
        Assert.Equal(a.GetHashCode(), sameSource.GetHashCode());

        KafkaFuture x = new KafkaFuture(Completed());
        KafkaFuture y = new KafkaFuture(Completed());
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        SyncCompletion<bool> pending = new SyncCompletion<bool>();
        KafkaFuture before = new KafkaFuture(pending);
        int hashBefore = before.GetHashCode();
        Assert.True(pending.TrySetResult(true));
        KafkaFuture after = new KafkaFuture(pending);
        Assert.True(before == after);
        Assert.Equal(hashBefore, after.GetHashCode());

        KafkaFuture d1 = default(KafkaFuture);
        KafkaFuture d2 = new KafkaFuture();
        Assert.True(d1.Equals(d2));
        Assert.True(d1 == d2);
        Assert.False(d1 != d2);
        Assert.Equal(d1.GetHashCode(), d2.GetHashCode());
        Assert.False(d1.Equals(a));
        Assert.False(a.Equals(d1));
        Assert.True(d1 != a);
        Assert.True(a != d1);
        Assert.False(d1 == a);

        object boxedEqual = sameSource;
        object boxedDifferent = y;
        object rawSource = source;
        object typedOverTheSameSource = new KafkaFuture<bool>(source);
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawSource));
        Assert.True(a.Equals(typedOverTheSameSource));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void Erasure_WrapsTheSameSource_SoErasuresOfOneFutureAreEqual()
    {
        SyncCompletion<bool> source = new SyncCompletion<bool>();
        KafkaFuture<bool> typed = new KafkaFuture<bool>(source);

        KafkaFuture first = typed.Erase();
        KafkaFuture second = typed.Erase();
        KafkaFuture direct = new KafkaFuture(source);

        Assert.True(first == second);
        Assert.True(first.Equals(second));
        Assert.True(first == direct);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.Equal(typed.GetHashCode(), first.GetHashCode());

        KafkaFuture other = new KafkaFuture<bool>(new SyncCompletion<bool>()).Erase();
        Assert.True(first != other);

        Assert.True(default(KafkaFuture<bool>).Erase() == default(KafkaFuture));
        Assert.True(default(KafkaFuture<RecordMetadata>).Erase() == default(KafkaFuture));
    }

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void Erasure_CompletesExactlyWhenTheTypedFutureDoes_WithItsOutcome(string outcome)
    {
        SyncCompletion<RecordMetadata> source = new SyncCompletion<RecordMetadata>();
        KafkaFuture<RecordMetadata> typed = new KafkaFuture<RecordMetadata>(source);
        KafkaFuture erased = typed.Erase();

        Assert.False(typed.IsDone);
        Assert.False(erased.IsDone);
        BlockingCall waiting = BlockingCall.StartWait(() => erased.Get());
        waiting.AssertBlocked();

        KafkaException cause = new KafkaException(7, "The operation failed.", isRetriable: true);
        Assert.True(outcome == "result"
            ? source.TrySetResult(new RecordMetadata("erasure-topic", 0, 1L, 1_700_000_000_000L))
            : source.TrySetException(cause));

        Assert.True(typed.IsDone);
        Assert.True(erased.IsDone);
        object? observed = waiting.Join();
        if (outcome == "result")
        {
            Assert.Same(BlockingCall.Returned, observed);
        }
        else
        {
            Assert.Same(cause, observed);
            Assert.Same(cause, BlockingCall.RunGet(() => typed.Get()));
        }
    }

    [Fact]
    public void Default_EveryMemberButEqualityAndToString_ThrowsInvalidOperationException()
    {
        KafkaFuture future = default(KafkaFuture);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        AssertDefaultThrows(BlockingCall.RunWait(() => future.Get(TimeSpan.Zero)));
        AssertDefaultThrows(BlockingCall.RunWait(() => future.Get(cancellation.Token)));
        AssertDefaultThrows(BlockingCall.RunWait(() => future.GetNow()));
        AssertDefaultThrows(BlockingCall.RunGet(() => future.IsCancelled));
        AssertDefaultThrows(BlockingCall.RunGet(() => future.IsCompletedExceptionally));
        AssertDefaultThrows(BlockingCall.RunGet(() => future.Cancel(true)));
        AssertDefaultThrows(BlockingCall.RunGet(() => future.ThenApply(() => 1)));
        AssertDefaultThrows(BlockingCall.RunGet(() => future.WhenComplete(error => { })));
        Assert.Equal(typeof(KafkaFuture).ToString(), future.ToString());
    }

    [Fact]
    public void ThenApplyAndWhenComplete_RejectANullDelegate()
    {
        KafkaFuture[] futures =
        {
            default(KafkaFuture),
            new KafkaFuture(new KafkaFutureImpl<object?>()),
            new KafkaFuture(new SyncCompletion<bool>()),
        };
        foreach (KafkaFuture future in futures)
        {
            Assert.Equal("function", Assert.Throws<ArgumentNullException>(() => future.ThenApply<int>(null!)).ParamName);
            Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => future.WhenComplete(null!)).ParamName);
        }
    }

    [Theory]
    [InlineData("succeeded")]
    [InlineData("failed")]
    public void OverASource_TheMembersThatNeedACompletionSignal_ThrowNotSupportedException_OnlyWhilePending(
        string outcome)
    {
        KafkaException failure = new KafkaException(36, "The operation failed.", isRetriable: false);
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        KafkaFuture future = new KafkaFuture(completion);
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        AssertNotSupported("ThenApply", BlockingCall.RunGet(() => future.ThenApply(() => 1)));
        AssertNotSupported("WhenComplete", BlockingCall.RunGet(() => future.WhenComplete(error => { })));
        AssertNotSupported("Get", BlockingCall.RunWait(() => future.Get(TimeSpan.FromSeconds(1))));
        AssertNotSupported("Get", BlockingCall.RunWait(() => future.Get(cancellation.Token)));
        AssertNotSupported("AllOf", BlockingCall.RunGet(() => KafkaFuture.AllOf(future)));

        Assert.True(outcome == "succeeded" ? completion.TrySetResult(true) : completion.TrySetException(failure));
        cancellation.Cancel();
        int appliedOn = 0;
        int completedOn = 0;
        Exception? seenError = null;

        BlockingCall applying = BlockingCall.StartGet(() => future.ThenApply(() =>
        {
            appliedOn = Environment.CurrentManagedThreadId;
            return 1;
        }));
        KafkaFuture<int> applied = Assert.IsType<KafkaFuture<int>>(applying.Join());
        BlockingCall completing = BlockingCall.StartGet(() => future.WhenComplete(error =>
        {
            completedOn = Environment.CurrentManagedThreadId;
            seenError = error;
        }));
        KafkaFuture completed = Assert.IsType<KafkaFuture>(completing.Join());
        KafkaFuture all = Assert.IsType<KafkaFuture>(BlockingCall.RunGet(() => KafkaFuture.AllOf(future)));

        Assert.True(applied.IsDone);
        Assert.True(completed.IsDone);
        Assert.True(all.IsDone);
        Assert.Equal(completing.CallingThreadId, completedOn);
        if (outcome == "succeeded")
        {
            Assert.Equal(applying.CallingThreadId, appliedOn);
            Assert.Equal(1, Assert.IsType<int>(BlockingCall.RunGet(() => applied.Get())));
            Assert.Null(seenError);
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => completed.Get()));
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => all.Get()));
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get(TimeSpan.Zero)));
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get(TimeSpan.FromSeconds(1))));
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get(cancellation.Token)));
        }
        else
        {
            Assert.Equal(0, appliedOn);
            AssertFailedWith(failure, BlockingCall.RunGet(() => applied.Get()));
            Assert.Same(failure, seenError);
            AssertFailedWith(failure, BlockingCall.RunWait(() => completed.Get()));
            AssertFailedWith(failure, BlockingCall.RunWait(() => all.Get()));
            AssertFailedWith(failure, BlockingCall.RunWait(() => future.Get(TimeSpan.Zero)));
            AssertFailedWith(failure, BlockingCall.RunWait(() => future.Get(TimeSpan.FromSeconds(1))));
            AssertFailedWith(failure, BlockingCall.RunWait(() => future.Get(cancellation.Token)));
        }
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("succeeded")]
    [InlineData("failed")]
    [InlineData("canceled")]
    public void OverASource_StateReadsFollowIt_AndCancelChangesNothing(string state)
    {
        Exception failure = state == "canceled"
            ? new OperationCanceledException("The operation was canceled by its owner.")
            : new KafkaException(36, "The operation failed.", isRetriable: false);
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        if (state == "succeeded")
        {
            Assert.True(completion.TrySetResult(true));
        }
        else if (state != "pending")
        {
            Assert.True(completion.TrySetException(failure));
        }

        KafkaFuture future = new KafkaFuture(completion);
        bool failed = state == "failed" || state == "canceled";

        Assert.False(future.Cancel(true));
        Assert.Equal(state != "pending", future.IsDone);
        Assert.False(future.IsCancelled);
        Assert.Equal(failed, future.IsCompletedExceptionally);
        if (failed)
        {
            Assert.Same(failure, Assert.Throws(failure.GetType(), () => future.GetNow()));
            Assert.Equal(
                "KafkaFuture{value=null,exception=" + failure.GetType().FullName + ": " + failure.Message + ",done=true}",
                future.ToString());
            Assert.Same(failure, BlockingCall.RunWait(() => future.Get()));
        }
        else
        {
            future.GetNow();
            Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());
        }
    }

    [Fact]
    public void OverAnImpl_ThenApplyAndWhenComplete_FollowTheFuture()
    {
        KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
        KafkaFuture future = new KafkaFuture(impl);
        int calls = 0;
        KafkaFuture<int> applied = future.ThenApply(() => Interlocked.Increment(ref calls) + 41);
        Exception? seen = new InvalidOperationException("The action did not run.");
        KafkaFuture completed = future.WhenComplete(error => seen = error);
        Assert.False(applied.IsDone);
        Assert.False(completed.IsDone);

        Assert.True(impl.Complete(null));
        Assert.Equal(42, applied.GetNow(-1));
        Assert.Null(seen);
        completed.GetNow();
        Assert.True(completed.IsDone);
        Assert.Equal(1, calls);

        KafkaFutureImpl<object?> failing = new KafkaFutureImpl<object?>();
        KafkaFuture failingFuture = new KafkaFuture(failing);
        bool ran = false;
        KafkaFuture<int> skipped = failingFuture.ThenApply(() =>
        {
            ran = true;
            return 0;
        });
        Exception? seenFailure = null;
        KafkaFuture after = failingFuture.WhenComplete(error => seenFailure = error);
        KafkaException cause = new KafkaException(36, "The operation failed.", isRetriable: false);
        Assert.True(failing.CompleteWithError(cause));
        Assert.False(ran);
        Assert.Same(cause, Assert.Throws<KafkaException>(() => skipped.GetNow(-1)));
        Assert.Same(cause, seenFailure);
        Assert.Same(cause, Assert.Throws<KafkaException>(() => after.GetNow()));
    }

    [Fact]
    public void OverAnImpl_TheGetForms_CancelAndToString_FollowTheFuture()
    {
        KafkaFuture future = new KafkaFuture(new KafkaFutureImpl<object?>());
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Assert.IsType<TimeoutException>(BlockingCall.RunWait(() => future.Get(TimeSpan.Zero)));
        future.GetNow();
        BlockingCall waiting = BlockingCall.StartWait(() => future.Get(cancellation.Token));
        waiting.AssertBlocked();
        cancellation.Cancel();
        OperationCanceledException ended = Assert.IsType<OperationCanceledException>(waiting.Join());
        Assert.Equal(cancellation.Token, ended.CancellationToken);
        Assert.False(future.IsDone);
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());

        Assert.True(future.Cancel(true));
        Assert.True(future.IsDone);
        Assert.True(future.IsCancelled);
        Assert.True(future.IsCompletedExceptionally);
        Assert.True(future.Cancel(false));
        Exception cancelled = Assert.IsType<OperationCanceledException>(BlockingCall.RunWait(() => future.Get()));
        Assert.Same(cancelled, BlockingCall.RunWait(() => future.Get(TimeSpan.FromSeconds(1))));
        Assert.Same(cancelled, Assert.Throws<OperationCanceledException>(() => future.GetNow()));
        Assert.Equal(
            "KafkaFuture{value=null,exception=System.OperationCanceledException: " + cancelled.Message + ",done=true}",
            future.ToString());

        KafkaFuture done = KafkaFuture.CompletedFuture(5);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => done.Get(TimeSpan.Zero)));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => done.Get(cancellation.Token)));
        Assert.False(done.Cancel(true));
        Assert.False(done.IsCancelled);
    }

    [Fact]
    public void AllOf_TakesMixedTypes_AnArrayOfOneType_AndNoFutures()
    {
        KafkaFutureImpl<int> number = new KafkaFutureImpl<int>();
        KafkaFutureImpl<string> text = new KafkaFutureImpl<string>();
        KafkaFuture mixed = KafkaFuture.AllOf(new KafkaFuture<int>(number), new KafkaFuture<string>(text));
        Assert.False(mixed.IsDone);
        Assert.True(number.Complete(1));
        Assert.False(mixed.IsDone);
        Assert.True(text.Complete("one"));
        Assert.True(mixed.IsDone);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => mixed.Get()));

        KafkaFutureImpl<int> first = new KafkaFutureImpl<int>();
        KafkaFutureImpl<int> second = new KafkaFutureImpl<int>();
        KafkaFuture<int>[] oneType = { new KafkaFuture<int>(first), new KafkaFuture<int>(second) };
        KafkaFuture all = KafkaFuture.AllOf(oneType);
        KafkaException cause = new KafkaException(36, "The operation failed.", isRetriable: false);
        Assert.True(first.CompleteWithError(cause));
        Assert.False(all.IsDone);
        Assert.True(second.Complete(2));
        Assert.Same(cause, BlockingCall.RunWait(() => all.Get()));
        Assert.True(all.IsCompletedExceptionally);
        Assert.False(all.IsCancelled);

        KafkaFuture none = KafkaFuture.AllOf();
        Assert.True(none.IsDone);
        Assert.False(none.IsCompletedExceptionally);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => none.Get()));
        Assert.True(KafkaFuture.AllOf<int>().IsDone);

        Assert.Equal("futures", Assert.Throws<ArgumentNullException>(() => KafkaFuture.AllOf((KafkaFuture[])null!)).ParamName);
        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => KafkaFuture.AllOf((KafkaFuture<int>[])null!)).ParamName);
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithJavasKafkaFutureMembers()
    {
        Type type = typeof(KafkaFuture);

        Assert.True(type.IsValueType);
        Assert.True(type.IsPublic);
        Assert.False(type.IsGenericType);
        Assert.Equal("Confluent.Kafka", type.Namespace);
        Assert.Contains(
            type.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        Assert.Equal(new[] { typeof(IEquatable<KafkaFuture>) }, type.GetInterfaces());

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.KafkaFuture WhenComplete(System.Action`1[System.Exception])",
                "Confluent.Kafka.KafkaFuture`1[TR] ThenApply(System.Func`1[TR])",
                "System.Boolean Cancel(System.Boolean)",
                "System.Boolean Equals(Confluent.Kafka.KafkaFuture)",
                "System.Boolean Equals(System.Object)",
                "System.Boolean get_IsCancelled()",
                "System.Boolean get_IsCompletedExceptionally()",
                "System.Boolean get_IsDone()",
                "System.Int32 GetHashCode()",
                "System.String ToString()",
                "System.Void Get()",
                "System.Void Get(System.Threading.CancellationToken)",
                "System.Void Get(System.TimeSpan)",
                "System.Void GetNow()",
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
                "Confluent.Kafka.KafkaFuture AllOf(Confluent.Kafka.KafkaFuture[])",
                "Confluent.Kafka.KafkaFuture AllOf(Confluent.Kafka.KafkaFuture`1[T][])",
                "Confluent.Kafka.KafkaFuture`1[TU] CompletedFuture(TU)",
                "System.Boolean op_Equality(Confluent.Kafka.KafkaFuture, Confluent.Kafka.KafkaFuture)",
                "System.Boolean op_Inequality(Confluent.Kafka.KafkaFuture, Confluent.Kafka.KafkaFuture)",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        Type[] nested = type.GetNestedTypes(BindingFlags.Public).OrderBy(nestedType => nestedType.Name, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "BaseFunction`2", "BiConsumer`2" }, nested.Select(nestedType => nestedType.Name).ToArray());
        Assert.All(nested, nestedType => Assert.Equal(typeof(MulticastDelegate), nestedType.BaseType));
        Assert.Equal("TB Invoke(TA)", Signature(nested[0].GetMethod("Invoke")!));
        Assert.Equal("System.Void Invoke(TA, TB)", Signature(nested[1].GetMethod("Invoke")!));
        Assert.Equal(
            new[] { GenericParameterAttributes.Contravariant, GenericParameterAttributes.Covariant },
            nested[0].GetGenericArguments().Select(argument => argument.GenericParameterAttributes).ToArray());
        Assert.Equal(
            new[] { GenericParameterAttributes.Contravariant, GenericParameterAttributes.Contravariant },
            nested[1].GetGenericArguments().Select(argument => argument.GenericParameterAttributes).ToArray());

        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.All(constructors, constructor => Assert.True(constructor.IsAssembly));
        Assert.Equal(
            new[] { typeof(KafkaFutureImpl).ToString(), typeof(SyncFutureSource).ToString() },
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
            new[] { typeof(KafkaFutureImpl) + " _impl", typeof(SyncFutureSource) + " _source" },
            fields.Select(field => field.FieldType + " " + field.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray());
    }

    private static void AssertDefaultThrows(object? outcome)
    {
        InvalidOperationException thrown = Assert.IsType<InvalidOperationException>(outcome);
        Assert.Equal(DefaultValueMessage, thrown.Message);
    }

    private static void AssertNotSupported(string member, object? outcome)
    {
        NotSupportedException thrown = Assert.IsType<NotSupportedException>(outcome);
        Assert.Equal(
            "KafkaFuture." + member + " is not supported on a pending future returned by IProducer.Send or an IAdmin "
            + "result.",
            thrown.Message);
    }

    private static void AssertFailedWith(KafkaException failure, object? outcome)
    {
        KafkaException thrown = Assert.IsType<KafkaException>(outcome);
        Assert.Same(failure, thrown);
        Assert.Equal("The operation failed.", thrown.Message);
    }

    private static string[] Signatures(Type type, BindingFlags flags) =>
        type.GetMethods(flags).Select(Signature).OrderBy(signature => signature, StringComparer.Ordinal).ToArray();

    private static SyncCompletion<bool> Completed()
    {
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        Assert.True(completion.TrySetResult(true));
        return completion;
    }

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
