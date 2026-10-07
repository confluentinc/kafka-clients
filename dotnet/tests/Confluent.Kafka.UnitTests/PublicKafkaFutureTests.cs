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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public <see cref="KafkaFuture{T}"/> on its own (M11/P4.2 S1, PLAN §7 N1–N3, N9, N10), built through its
/// <c>internal</c> constructor over a <see cref="SyncCompletion{T}"/>: the default-value guard, the same-instance
/// contract of <see cref="KafkaFuture{T}.Get"/> on success and on failure, D6's reference-identity equality, and the
/// approved member set (no <c>Get(TimeSpan)</c>: D7 deferred, FU-4). The latch mechanics — blocking, first
/// completion wins, the set/get race — are <c>Interop/SyncCompletionTests</c> (N4–N6). Everything here is available
/// on the netstandard2.0 floor, so it also compiles on net462.
/// </summary>
public sealed class PublicKafkaFutureTests
{
    // PLAN §3, verbatim — the twin of AsyncKafkaFuture's.
    private const string DefaultValueMessage =
        "This KafkaFuture is a default value and carries no send; only IProducer.Send returns a usable one.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Default_Get_ThrowsInvalidOperationException_WithItsMessage()
    {
        KafkaFuture<RecordMetadata> viaDefault = default(KafkaFuture<RecordMetadata>);
        KafkaFuture<RecordMetadata> viaNew = new KafkaFuture<RecordMetadata>();

        // Twice each: a call on a default value latches nothing, so it stays unusable.
        for (int call = 0; call < 2; call++)
        {
            AssertDefaultGetThrows(() => viaDefault.Get());
            AssertDefaultGetThrows(() => viaNew.Get());
        }

        // A value-type result does not change it: the guard is on the missing completion, not on a null T.
        AssertDefaultGetThrows(() => default(KafkaFuture<long>).Get());
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

        // A copy of the struct shares the completion, so it must read the very same instance.
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

        // Assert.Throws matches the exact type, so an AggregateException (or any other wrapper) fails here.
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
        // Same completion → equal, ==, same hash.
        SyncCompletion<RecordMetadata> completion = Completed(NewMetadata());
        KafkaFuture<RecordMetadata> a = new KafkaFuture<RecordMetadata>(completion);
        KafkaFuture<RecordMetadata> sameCompletion = new KafkaFuture<RecordMetadata>(completion);
        Assert.True(a.Equals(sameCompletion));
        Assert.True(a == sameCompletion);
        Assert.False(a != sameCompletion);
        Assert.Equal(a.GetHashCode(), sameCompletion.GetHashCode());

        // Different completions, both completed with the SAME value → not equal: identity, never the result.
        RecordMetadata shared = NewMetadata();
        SyncCompletion<RecordMetadata> firstCompletion = Completed(shared);
        SyncCompletion<RecordMetadata> secondCompletion = Completed(shared);
        KafkaFuture<RecordMetadata> x = new KafkaFuture<RecordMetadata>(firstCompletion);
        KafkaFuture<RecordMetadata> y = new KafkaFuture<RecordMetadata>(secondCompletion);
        Assert.Same(x.Get(), y.Get());
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        // A pending completion compares by identity too: completing it later changes nothing.
        SyncCompletion<RecordMetadata> pending = new SyncCompletion<RecordMetadata>();
        KafkaFuture<RecordMetadata> before = new KafkaFuture<RecordMetadata>(pending);
        int hashBefore = before.GetHashCode();
        Assert.True(pending.TrySetResult(shared));
        KafkaFuture<RecordMetadata> after = new KafkaFuture<RecordMetadata>(pending);
        Assert.True(before == after);
        Assert.Equal(hashBefore, after.GetHashCode());

        // default == default; default != non-default (both directions).
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

        // Equals(object): a boxed equal other, a boxed different one, a non-future (the raw completion itself),
        // a string, and null.
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
    public void Shape_IsAPublicReadonlyStruct_WithOnlyTheApprovedMembers()
    {
        Type open = typeof(KafkaFuture<>);
        Type type = typeof(KafkaFuture<RecordMetadata>);

        Assert.True(type.IsValueType);
        Assert.True(open.IsPublic);
        Assert.Equal("Confluent.Kafka", open.Namespace);

        // `readonly struct` is encoded as IsReadOnlyAttribute; match by name, since netstandard2.0
        // builds embed their own copy of the attribute type.
        Assert.Contains(
            open.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        // No constraint on T.
        Type typeParameter = Assert.Single(open.GetGenericArguments());
        Assert.Equal(GenericParameterAttributes.None, typeParameter.GenericParameterAttributes);
        Assert.Empty(typeParameter.GetGenericParameterConstraints());

        // Exactly one interface: IEquatable<self> (D6).
        Assert.Equal(new[] { typeof(IEquatable<KafkaFuture<RecordMetadata>>) }, type.GetInterfaces());

        // Declared public instance methods: Get + D6's three. No Get(TimeSpan) (D7 deferred, FU-4), no
        // GetAwaiter / ConfigureAwait, no IsDone / IsCancelled / Cancel, no ToString override.
        string[] instanceMethods = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(Signature)
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[]
            {
                "Confluent.Kafka.RecordMetadata Get()",
                "System.Boolean Equals(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean Equals(System.Object)",
                "System.Int32 GetHashCode()",
            },
            instanceMethods);

        // Get has exactly one overload, at any visibility: the parameterless one.
        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        MethodInfo get = Assert.Single(type.GetMethods(AnyInstance), method => method.Name == "Get");
        Assert.Empty(get.GetParameters());
        Assert.Null(type.GetMethod("Get", AnyInstance, null, new[] { typeof(TimeSpan) }, null));
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.Equal(typeof(ValueType), type.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        // Declared public static methods: the two operators only.
        string[] staticMethods = type
            .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(Signature)
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[]
            {
                "System.Boolean op_Equality(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata], "
                    + "Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean op_Inequality(Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata], "
                    + "Confluent.Kafka.KafkaFuture`1[Confluent.Kafka.RecordMetadata])",
            },
            staticMethods);

        // No public constructor; the one internal constructor takes the completion.
        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo constructor = Assert.Single(type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(constructor.IsAssembly);
        Assert.Equal(
            new[] { typeof(SyncCompletion<RecordMetadata>) },
            constructor.GetParameters().Select(parameter => parameter.ParameterType).ToArray());

        // No public fields or properties; the one private field is a readonly managed completion — never a native
        // pointer (S-1).
        Assert.Empty(type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        FieldInfo field = Assert.Single(type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(field.IsPrivate);
        Assert.True(field.IsInitOnly);
        Assert.Equal(typeof(SyncCompletion<RecordMetadata>), field.FieldType);
    }

    private static void AssertDefaultGetThrows(Func<object?> get)
    {
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(get);
        Assert.Equal(DefaultValueMessage, thrown.Message);
    }

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

    // "<return type> <name>(<parameter types>)", with framework type names (Type.ToString()).
    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
