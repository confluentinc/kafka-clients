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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public non-generic <see cref="KafkaFuture"/> (M15/P14 S1, PLAN §2.3) — Java's <c>KafkaFuture&lt;Void&gt;</c> —
/// built through its <c>internal</c> constructor over a source: the default-value guard on <see cref="KafkaFuture.Get"/>
/// and <see cref="KafkaFuture.IsDone"/>, the same-instance failure contract, reference-identity equality mirroring
/// <see cref="KafkaFuture{T}"/>'s row for row, the erasure from <see cref="KafkaFuture{T}"/> (the same source, never a
/// new completion), and the approved member set. Every call that can block runs on a dedicated thread under a deadline
/// (<see cref="BlockingCall"/>).
/// </summary>
public sealed class PublicNonGenericKafkaFutureTests
{
    // M15/P14 S1, verbatim — the same text as KafkaFuture<T>'s.
    private const string DefaultValueMessage =
        "This KafkaFuture is a default value and is bound to no operation; only IProducer.Send and the IAdmin result "
        + "types return a usable one.";

    [Fact]
    public void Default_GetAndIsDone_ThrowInvalidOperationException_WithItsMessage()
    {
        KafkaFuture viaDefault = default(KafkaFuture);
        KafkaFuture viaNew = new KafkaFuture();

        // Twice each: a call on a default value latches nothing, so it stays unusable.
        for (int call = 0; call < 2; call++)
        {
            AssertDefaultThrows(BlockingCall.RunWait(() => viaDefault.Get()));
            AssertDefaultThrows(BlockingCall.RunWait(() => viaNew.Get()));
            AssertDefaultThrows(BlockingCall.RunGet(() => viaDefault.IsDone));
            AssertDefaultThrows(BlockingCall.RunGet(() => viaNew.IsDone));
        }

        // The text is KafkaFuture<T>'s own, so the two default guards cannot drift apart.
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

        // The exact instance on every call: no AggregateException, no copy.
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
        // Same source → equal, ==, same hash.
        SyncCompletion<bool> source = Completed();
        KafkaFuture a = new KafkaFuture(source);
        KafkaFuture sameSource = new KafkaFuture(source);
        Assert.True(a.Equals(sameSource));
        Assert.True(a == sameSource);
        Assert.False(a != sameSource);
        Assert.Equal(a.GetHashCode(), sameSource.GetHashCode());

        // Different sources, both completed the same way → not equal: identity, never the outcome.
        KafkaFuture x = new KafkaFuture(Completed());
        KafkaFuture y = new KafkaFuture(Completed());
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        // A pending source compares by identity too: completing it later changes nothing.
        SyncCompletion<bool> pending = new SyncCompletion<bool>();
        KafkaFuture before = new KafkaFuture(pending);
        int hashBefore = before.GetHashCode();
        Assert.True(pending.TrySetResult(true));
        KafkaFuture after = new KafkaFuture(pending);
        Assert.True(before == after);
        Assert.Equal(hashBefore, after.GetHashCode());

        // default == default; default != non-default (both directions).
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

        // Equals(object): a boxed equal other, a boxed different one, the raw source itself, a typed future over the
        // same source (a different type, so never equal), a string, and null.
        object boxedEqual = sameSource;
        object boxedDifferent = y;
        object rawSource = source;
        object typedOverTheSameSource = new KafkaFuture<bool>(source);
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawSource));
        Assert.False(a.Equals(typedOverTheSameSource));
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

        // Two erasures of one future are equal, with one hash, and equal to a future built over the source itself —
        // and their hash is the typed future's: the same object stands behind all of them.
        Assert.True(first == second);
        Assert.True(first.Equals(second));
        Assert.True(first == direct);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.Equal(typed.GetHashCode(), first.GetHashCode());

        // Erasures of different futures are not equal.
        KafkaFuture other = new KafkaFuture<bool>(new SyncCompletion<bool>()).Erase();
        Assert.True(first != other);

        // A default typed future erases to a default one.
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
            // The typed future's own failure, the same instance.
            Assert.Same(cause, observed);
            Assert.Same(cause, BlockingCall.RunGet(() => typed.Get()));
        }
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithOnlyTheApprovedMembers()
    {
        Type type = typeof(KafkaFuture);

        Assert.True(type.IsValueType);
        Assert.True(type.IsPublic);
        Assert.False(type.IsGenericType);
        Assert.Equal("Confluent.Kafka", type.Namespace);
        Assert.Contains(
            type.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        // Exactly one interface: IEquatable<self>, as KafkaFuture<T>.
        Assert.Equal(new[] { typeof(IEquatable<KafkaFuture>) }, type.GetInterfaces());

        // Declared public instance methods: Get, IsDone's getter + the equality three — KafkaFuture<T>'s set without a
        // value. No Get(TimeSpan), no GetAwaiter / ConfigureAwait, no conversion, no ToString override.
        string[] instanceMethods = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(Signature)
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[]
            {
                "System.Boolean Equals(Confluent.Kafka.KafkaFuture)",
                "System.Boolean Equals(System.Object)",
                "System.Boolean get_IsDone()",
                "System.Int32 GetHashCode()",
                "System.Void Get()",
            },
            instanceMethods);

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        MethodInfo get = Assert.Single(type.GetMethods(AnyInstance), method => method.Name == "Get");
        Assert.Empty(get.GetParameters());
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.Equal(typeof(ValueType), type.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        // Declared public static methods: the two operators only — no implicit / explicit conversion operator.
        string[] staticMethods = type
            .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(Signature)
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[]
            {
                "System.Boolean op_Equality(Confluent.Kafka.KafkaFuture, Confluent.Kafka.KafkaFuture)",
                "System.Boolean op_Inequality(Confluent.Kafka.KafkaFuture, Confluent.Kafka.KafkaFuture)",
            },
            staticMethods);

        // No public constructor; the one internal constructor takes the non-generic source.
        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo constructor = Assert.Single(type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(constructor.IsAssembly);
        Assert.Equal(
            new[] { typeof(SyncFutureSource) },
            constructor.GetParameters().Select(parameter => parameter.ParameterType).ToArray());

        // No public fields; one public property, the read-only IsDone. The one private field is a readonly source.
        Assert.Empty(type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        PropertyInfo isDone = Assert.Single(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        Assert.Equal("IsDone", isDone.Name);
        Assert.Equal(typeof(bool), isDone.PropertyType);
        Assert.False(isDone.CanWrite);
        FieldInfo field = Assert.Single(type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(field.IsPrivate);
        Assert.True(field.IsInitOnly);
        Assert.Equal(typeof(SyncFutureSource), field.FieldType);
    }

    private static void AssertDefaultThrows(object? outcome)
    {
        InvalidOperationException thrown = Assert.IsType<InvalidOperationException>(outcome);
        Assert.Equal(DefaultValueMessage, thrown.Message);
    }

    private static SyncCompletion<bool> Completed()
    {
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        Assert.True(completion.TrySetResult(true));
        return completion;
    }

    // "<return type> <name>(<parameter types>)", with framework type names (Type.ToString()).
    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
