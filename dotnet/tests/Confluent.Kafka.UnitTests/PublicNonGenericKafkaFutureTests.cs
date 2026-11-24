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
/// Tests the public non-generic <see cref="KafkaFuture"/>, Java's <c>KafkaFuture&lt;Void&gt;</c>, which every
/// <see cref="KafkaFuture{T}"/> is: a <c>default</c> one is <see langword="null"/>, the same-instance failure contract,
/// identity equality, a typed future seen as one, the static factories and their null checks, and the member set.
/// </summary>
public sealed class PublicNonGenericKafkaFutureTests
{
    [Fact]
    public void Default_IsNull()
    {
        KafkaFuture? declared = default;

        Assert.Null(declared);
        Assert.Null(default(KafkaFuture));
        Assert.Null((KafkaFuture?)default(KafkaFuture<RecordMetadata>));
    }

    [Fact]
    public void Get_BlocksUntilTheSourceSucceeds_ThenReturns_OnEveryCall()
    {
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        KafkaFuture future = completion;

        BlockingCall waiting = BlockingCall.StartWait(() => future.Get());
        waiting.AssertBlocked();

        Assert.True(completion.Complete(true));

        Assert.Same(BlockingCall.Returned, waiting.Join());
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get()));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => future.Get()));
    }

    [Fact]
    public void Get_AfterFailure_ThrowsAnExecutionException_AroundTheSameInstance()
    {
        const string Message = "The operation failed: the broker rejected it.";
        KafkaException cause = new Common.Errors.TopicExistsException(Message);
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        KafkaFuture future = completion;

        BlockingCall waiting = BlockingCall.StartWait(() => future.Get());
        waiting.AssertBlocked();
        Assert.True(completion.CompleteWithError(cause));

        Assert.Same(cause, FutureAssert.Cause<TopicExistsException>(Assert.IsAssignableFrom<Exception>(waiting.Join())));
        for (int call = 0; call < 2; call++)
        {
            TopicExistsException thrown = FutureAssert.Cause<TopicExistsException>(
                Assert.IsAssignableFrom<Exception>(BlockingCall.RunWait(() => future.Get())));
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
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        KafkaFuture future = completion;

        Assert.False(future.IsDone());
        Assert.False(future.IsDone());

        Assert.True(outcome == "result"
            ? completion.Complete(true)
            : completion.CompleteWithError(new KafkaException("The operation failed.")));

        Assert.True(future.IsDone());
        Assert.True(future.IsDone());
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheSource()
    {
        KafkaFutureImpl<bool> source = Completed();
        KafkaFuture a = source;
        KafkaFuture sameSource = source;
        Assert.True(a.Equals(sameSource));
        Assert.True(a == sameSource);
        Assert.False(a != sameSource);
        Assert.Equal(a.GetHashCode(), sameSource.GetHashCode());

        KafkaFuture x = Completed();
        KafkaFuture y = Completed();
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        KafkaFutureImpl<bool> pending = new KafkaFutureImpl<bool>();
        KafkaFuture before = pending;
        int hashBefore = before.GetHashCode();
        Assert.True(pending.Complete(true));
        KafkaFuture after = pending;
        Assert.True(before == after);
        Assert.Equal(hashBefore, after.GetHashCode());

        KafkaFuture? n1 = null;
        KafkaFuture? n2 = default;
        Assert.True(n1 == n2);
        Assert.False(n1 != n2);
        Assert.True(Equals(n1, n2));
        Assert.False(a.Equals(n1));
        Assert.True(n1 != a);
        Assert.True(a != n1);
        Assert.False(n1 == a);

        object boxedEqual = sameSource;
        object boxedDifferent = y;
        object rawSource = source;
        KafkaFuture<bool> typedOverTheSameSource = source;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.Same(rawSource, a);
        Assert.True(a.Equals(rawSource));
        Assert.True(a.Equals(typedOverTheSameSource));
        Assert.True(a == typedOverTheSameSource);
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void APlainView_IsTheTypedFutureItself()
    {
        KafkaFutureImpl<bool> source = new KafkaFutureImpl<bool>();
        KafkaFuture<bool> typed = source;

        KafkaFuture first = typed;
        KafkaFuture second = typed;
        KafkaFuture direct = source;

        Assert.Same(typed, first);
        Assert.Same(first, second);
        Assert.True(first == second);
        Assert.True(first.Equals(second));
        Assert.True(first == direct);
        Assert.Equal(typed.GetHashCode(), first.GetHashCode());

        KafkaFuture other = new KafkaFutureImpl<bool>();
        Assert.True(first != other);

        KafkaFuture? plainNull = default(KafkaFuture<bool>);
        Assert.Null(plainNull);
        Assert.True(plainNull == default(KafkaFuture));
    }

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void APlainView_CompletesExactlyWhenTheTypedFutureDoes_WithItsOutcome(string outcome)
    {
        KafkaFutureImpl<RecordMetadata> source = new KafkaFutureImpl<RecordMetadata>();
        KafkaFuture<RecordMetadata> typed = source;
        KafkaFuture erased = typed;

        Assert.False(typed.IsDone());
        Assert.False(erased.IsDone());
        BlockingCall waiting = BlockingCall.StartWait(() => erased.Get());
        waiting.AssertBlocked();

        KafkaException cause = new Common.Errors.TimeoutException("The operation failed.");
        Assert.True(outcome == "result"
            ? source.Complete(
                new RecordMetadata(new TopicPartition("erasure-topic", 0), 1L, 0, 1_700_000_000_000L, -1, -1))
            : source.CompleteWithError(cause));

        Assert.True(typed.IsDone());
        Assert.True(erased.IsDone());
        object? observed = waiting.Join();
        if (outcome == "result")
        {
            Assert.Same(BlockingCall.Returned, observed);
        }
        else
        {
            Assert.Same(cause, FutureAssert.Unwrapped(observed));
            Assert.Same(cause, FutureAssert.Unwrapped(BlockingCall.RunGet(() => typed.Get())));
        }
    }

    [Fact]
    public void AllOf_ANullFuture_ThrowsArgumentNullException_NamingFutures_BothOverloads()
    {
        KafkaFutureImpl<int> live = new KafkaFutureImpl<int>();
        string expected = new ArgumentNullException("futures", "futures must not contain null").Message;

        foreach (Func<KafkaFuture> call in new Func<KafkaFuture>[]
        {
            () => KafkaFuture.AllOf(live, null!),
            () => KafkaFuture.AllOf(null!, live),
            () => KafkaFuture.AllOf(new KafkaFuture[] { null! }),
            () => KafkaFuture.AllOf<int>(live, null!),
            () => KafkaFuture.AllOf(new KafkaFuture<int>[] { live, null! }),
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
        KafkaFuture[] futures =
        {
            new KafkaFutureImpl<object?>(),
            KafkaFuture.CompletedFuture(1),
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
    public void OnceDone_ThenApplyWhenCompleteAndAllOf_RunInsideTheCall(string outcome)
    {
        KafkaException failure = new Common.Errors.TopicExistsException("The operation failed.");
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        KafkaFuture future = completion;
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Assert.True(outcome == "succeeded" ? completion.Complete(true) : completion.CompleteWithError(failure));
        cancellation.Cancel();
        int appliedOn = 0;
        int completedOn = 0;
        Exception? seenError = null;

        BlockingCall applying = BlockingCall.StartGet(() => future.ThenApply(() =>
        {
            appliedOn = Environment.CurrentManagedThreadId;
            return 1;
        }));
        KafkaFuture<int> applied = Assert.IsAssignableFrom<KafkaFuture<int>>(applying.Join());
        BlockingCall completing = BlockingCall.StartGet(() => future.WhenComplete(error =>
        {
            completedOn = Environment.CurrentManagedThreadId;
            seenError = error;
        }));
        KafkaFuture completed = Assert.IsAssignableFrom<KafkaFuture>(completing.Join());
        KafkaFuture all = Assert.IsAssignableFrom<KafkaFuture>(BlockingCall.RunGet(() => KafkaFuture.AllOf(future)));

        Assert.True(applied.IsDone());
        Assert.True(completed.IsDone());
        Assert.True(all.IsDone());
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

    [Fact]
    public void OverAnImpl_ThenApplyAndWhenComplete_FollowTheFuture()
    {
        KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
        KafkaFuture future = impl;
        int calls = 0;
        KafkaFuture<int> applied = future.ThenApply(() => Interlocked.Increment(ref calls) + 41);
        Exception? seen = new InvalidOperationException("The action did not run.");
        KafkaFuture completed = future.WhenComplete(error => seen = error);
        Assert.False(applied.IsDone());
        Assert.False(completed.IsDone());

        Assert.True(impl.Complete(null));
        Assert.Equal(42, applied.GetNow(-1));
        Assert.Null(seen);
        completed.GetNow();
        Assert.True(completed.IsDone());
        Assert.Equal(1, calls);

        KafkaFutureImpl<object?> failing = new KafkaFutureImpl<object?>();
        KafkaFuture failingFuture = failing;
        bool ran = false;
        KafkaFuture<int> skipped = failingFuture.ThenApply(() =>
        {
            ran = true;
            return 0;
        });
        Exception? seenFailure = null;
        KafkaFuture after = failingFuture.WhenComplete(error => seenFailure = error);
        KafkaException cause = new Common.Errors.TopicExistsException("The operation failed.");
        Assert.True(failing.CompleteWithError(cause));
        Assert.False(ran);
        Assert.Same(cause, FutureAssert.Throws<TopicExistsException>(() => skipped.GetNow(-1)));
        Assert.Same(cause, seenFailure);
        Assert.Same(cause, FutureAssert.Throws<TopicExistsException>(() => after.GetNow()));
    }

    [Fact]
    public void OverAnImpl_TheGetForms_CancelAndToString_FollowTheFuture()
    {
        KafkaFuture future = new KafkaFutureImpl<object?>();
        using CancellationTokenSource cancellation = new CancellationTokenSource();

        Assert.IsType<FutureTimeoutException>(BlockingCall.RunWait(() => future.Get(TimeSpan.Zero)));
        future.GetNow();
        BlockingCall waiting = BlockingCall.StartWait(() => future.Get(cancellation.Token));
        waiting.AssertBlocked();
        cancellation.Cancel();
        OperationCanceledException ended = Assert.IsType<OperationCanceledException>(waiting.Join());
        Assert.Equal(cancellation.Token, ended.CancellationToken);
        Assert.False(future.IsDone());
        Assert.Equal("KafkaFuture{value=null,exception=null,done=false}", future.ToString());

        Assert.True(future.Cancel(true));
        Assert.True(future.IsDone());
        Assert.True(future.IsCancelled());
        Assert.True(future.IsCompletedExceptionally());
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
        Assert.False(done.IsCancelled());
    }

    [Fact]
    public void AllOf_TakesMixedTypes_AnArrayOfOneType_AndNoFutures()
    {
        KafkaFutureImpl<int> number = new KafkaFutureImpl<int>();
        KafkaFutureImpl<string> text = new KafkaFutureImpl<string>();
        KafkaFuture mixed = KafkaFuture.AllOf(number, text);
        Assert.False(mixed.IsDone());
        Assert.True(number.Complete(1));
        Assert.False(mixed.IsDone());
        Assert.True(text.Complete("one"));
        Assert.True(mixed.IsDone());
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => mixed.Get()));

        KafkaFutureImpl<int> first = new KafkaFutureImpl<int>();
        KafkaFutureImpl<int> second = new KafkaFutureImpl<int>();
        KafkaFuture<int>[] oneType = { first, second };
        KafkaFuture all = KafkaFuture.AllOf(oneType);
        KafkaException cause = new Common.Errors.TopicExistsException("The operation failed.");
        Assert.True(first.CompleteWithError(cause));
        Assert.False(all.IsDone());
        Assert.True(second.Complete(2));
        Assert.Same(
            cause, FutureAssert.Cause<TopicExistsException>(Assert.IsAssignableFrom<Exception>(BlockingCall.RunWait(() => all.Get()))));
        Assert.True(all.IsCompletedExceptionally());
        Assert.False(all.IsCancelled());

        KafkaFuture none = KafkaFuture.AllOf();
        Assert.True(none.IsDone());
        Assert.False(none.IsCompletedExceptionally());
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => none.Get()));
        Assert.True(KafkaFuture.AllOf<int>().IsDone());

        Assert.Equal("futures", Assert.Throws<ArgumentNullException>(() => KafkaFuture.AllOf((KafkaFuture[])null!)).ParamName);
        Assert.Equal(
            "futures",
            Assert.Throws<ArgumentNullException>(() => KafkaFuture.AllOf((KafkaFuture<int>[])null!)).ParamName);
    }

    [Fact]
    public void Shape_IsAPublicAbstractClass_WithJavasKafkaFutureMembers()
    {
        Type type = typeof(KafkaFuture);

        Assert.True(type.IsClass);
        Assert.True(type.IsAbstract);
        Assert.False(type.IsSealed);
        Assert.True(type.IsPublic);
        Assert.False(type.IsGenericType);
        Assert.Equal(typeof(object), type.BaseType);

        Assert.Equal(new[] { typeof(IEquatable<KafkaFuture>) }, type.GetInterfaces());

        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.Common.KafkaFuture WhenComplete(System.Action`1[System.Exception])",
                "Confluent.Kafka.Common.KafkaFuture`1[TR] ThenApply(System.Func`1[TR])",
                "System.Boolean Cancel(System.Boolean)",
                "System.Boolean Equals(Confluent.Kafka.Common.KafkaFuture)",
                "System.Boolean Equals(System.Object)",
                "System.Boolean IsCancelled()",
                "System.Boolean IsCompletedExceptionally()",
                "System.Boolean IsDone()",
                "System.Int32 GetHashCode()",
                "System.String ToString()",
                "System.Void Get()",
                "System.Void Get(System.Threading.CancellationToken)",
                "System.Void Get(System.TimeSpan)",
                "System.Void GetNow()",
            },
            Signatures(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));
        foreach (MethodInfo method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            bool abstractJavaMember = method.Name is not ("Get" or "Equals" or "GetHashCode");
            Assert.Equal(abstractJavaMember, method.IsAbstract);
        }

        Assert.All(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).Where(method => method.Name == "Get"),
            method => Assert.False(method.IsVirtual, method + " should not be virtual"));
        MethodInfo[] hooks = type.GetMethods(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
        Assert.Equal(3, hooks.Length);
        Assert.All(hooks, hook =>
        {
            Assert.True(hook.IsFamilyAndAssembly, hook + " should be private protected");
            Assert.True(hook.IsAbstract, hook + " should be abstract");
        });

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
                "Confluent.Kafka.Common.KafkaFuture AllOf(Confluent.Kafka.Common.KafkaFuture[])",
                "Confluent.Kafka.Common.KafkaFuture AllOf(Confluent.Kafka.Common.KafkaFuture`1[T][])",
                "Confluent.Kafka.Common.KafkaFuture`1[TU] CompletedFuture(TU)",
                "System.Boolean op_Equality(Confluent.Kafka.Common.KafkaFuture, Confluent.Kafka.Common.KafkaFuture)",
                "System.Boolean op_Inequality(Confluent.Kafka.Common.KafkaFuture, Confluent.Kafka.Common.KafkaFuture)",
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

        ConstructorInfo constructor = Assert.Single(type.GetConstructors(AnyInstance));
        Assert.True(constructor.IsFamilyAndAssembly);
        Assert.Empty(constructor.GetParameters());

        Assert.Empty(type.GetFields(AnyInstance | BindingFlags.Static | BindingFlags.DeclaredOnly));
        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
    }

    private static void AssertFailedWith(KafkaException failure, object? outcome)
    {
        TopicExistsException thrown = FutureAssert.Cause<TopicExistsException>(Assert.IsAssignableFrom<Exception>(outcome));
        Assert.Same(failure, thrown);
        Assert.Equal("The operation failed.", thrown.Message);
    }

    private static string[] Signatures(Type type, BindingFlags flags) =>
        type.GetMethods(flags).Select(Signature).OrderBy(signature => signature, StringComparer.Ordinal).ToArray();

    private static KafkaFutureImpl<bool> Completed()
    {
        KafkaFutureImpl<bool> completion = new KafkaFutureImpl<bool>();
        Assert.True(completion.Complete(true));
        return completion;
    }

    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
