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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public <see cref="AsyncKafkaFuture{T}"/> on its own (M11/P3.6 S1, PLAN §7 N1–N5), built through
/// its <c>internal</c> constructor: the D3 default-value guard, <see cref="AsyncKafkaFuture{T}.Get"/>'s
/// same-instance contract, <c>ConfigureAwait</c> on the task, D4's reference-identity equality, and the
/// approved member set (D4 in, D5/D6 out). Everything here is available on the netstandard2.0 floor, so
/// it also compiles on net462.
/// </summary>
public sealed class PublicAsyncKafkaFutureTests
{
    // D3, verbatim.
    private const string DefaultValueMessage =
        "This AsyncKafkaFuture is a default value and carries no send; only IAsyncProducer.Send returns a usable one.";

    [Fact]
    public void Default_Get_ThrowsInvalidOperationException_WithItsMessage()
    {
        AsyncKafkaFuture<RecordMetadata> viaDefault = default(AsyncKafkaFuture<RecordMetadata>);
        AsyncKafkaFuture<RecordMetadata> viaNew = new AsyncKafkaFuture<RecordMetadata>();

        AssertGetThrowsSynchronously(viaDefault);
        AssertGetThrowsSynchronously(viaNew);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void Get_ReturnsTheWrappedTask_TheSameInstanceOnEveryCall(string state)
    {
        Task<RecordMetadata> delivery = DeliveryIn(state);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(delivery);

        Task<RecordMetadata> first = future.Get();
        Task<RecordMetadata> second = future.Get();

        Assert.Same(delivery, first);
        Assert.Same(first, second);

        // Observe a faulted task's exception so it is never reported as unobserved by the finalizer.
        _ = delivery.Exception;
    }

    [Fact]
    public async Task Get_ConfigureAwait_FlowsThroughTheTask()
    {
        RecordMetadata metadata = NewMetadata();
        TaskCompletionSource<RecordMetadata> completion =
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
        AsyncKafkaFuture<RecordMetadata> future = new AsyncKafkaFuture<RecordMetadata>(completion.Task);

        // Start the ConfigureAwait(false) await while the delivery is still pending, then deliver.
        Task<RecordMetadata> awaiting = AwaitGetWithoutContext(future);
        Assert.False(awaiting.IsCompleted);
        completion.SetResult(metadata);

        Assert.Same(metadata, await awaiting);
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheDeliveryTask()
    {
        // Same task → equal, ==, same hash.
        Task<RecordMetadata> task = Delivered(NewMetadata());
        AsyncKafkaFuture<RecordMetadata> a = new AsyncKafkaFuture<RecordMetadata>(task);
        AsyncKafkaFuture<RecordMetadata> sameTask = new AsyncKafkaFuture<RecordMetadata>(task);
        Assert.True(a.Equals(sameTask));
        Assert.True(a == sameTask);
        Assert.False(a != sameTask);
        Assert.Equal(a.GetHashCode(), sameTask.GetHashCode());

        // Different tasks, both completed with the SAME value → not equal: identity, never the result.
        RecordMetadata shared = NewMetadata();
        Task<RecordMetadata> firstTask = Delivered(shared);
        Task<RecordMetadata> secondTask = Delivered(shared);
        Assert.NotSame(firstTask, secondTask);
        AsyncKafkaFuture<RecordMetadata> x = new AsyncKafkaFuture<RecordMetadata>(firstTask);
        AsyncKafkaFuture<RecordMetadata> y = new AsyncKafkaFuture<RecordMetadata>(secondTask);
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        // default == default; default != non-default (both directions).
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

        // Equals(object): a boxed equal other, a boxed different one, a non-future (the raw task
        // itself), and null.
        object boxedEqual = sameTask;
        object boxedDifferent = y;
        object rawTask = task;
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawTask));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithOnlyTheApprovedMembers()
    {
        Type open = typeof(AsyncKafkaFuture<>);
        Type type = typeof(AsyncKafkaFuture<RecordMetadata>);

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

        // Exactly one interface: IEquatable<self> (D4).
        Assert.Equal(new[] { typeof(IEquatable<AsyncKafkaFuture<RecordMetadata>>) }, type.GetInterfaces());

        // Declared public instance methods: Get + D4's three. No GetAwaiter / ConfigureAwait (D5),
        // no IsDone / Get(TimeSpan) / IsCancelled / Cancel (D6), no ToString override.
        string[] instanceMethods = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(Signature)
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[]
            {
                "System.Boolean Equals(Confluent.Kafka.AsyncKafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean Equals(System.Object)",
                "System.Int32 GetHashCode()",
                "System.Threading.Tasks.Task`1[Confluent.Kafka.RecordMetadata] Get()",
            },
            instanceMethods);

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
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
                "System.Boolean op_Equality(Confluent.Kafka.AsyncKafkaFuture`1[Confluent.Kafka.RecordMetadata], "
                    + "Confluent.Kafka.AsyncKafkaFuture`1[Confluent.Kafka.RecordMetadata])",
                "System.Boolean op_Inequality(Confluent.Kafka.AsyncKafkaFuture`1[Confluent.Kafka.RecordMetadata], "
                    + "Confluent.Kafka.AsyncKafkaFuture`1[Confluent.Kafka.RecordMetadata])",
            },
            staticMethods);

        // No public constructor; the one internal constructor takes the delivery task.
        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo constructor = Assert.Single(type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(constructor.IsAssembly);
        Assert.Equal(
            new[] { typeof(Task<RecordMetadata>) },
            constructor.GetParameters().Select(parameter => parameter.ParameterType).ToArray());

        // No public fields or properties; the one private field is a readonly Task<T> (D1).
        Assert.Empty(type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        FieldInfo field = Assert.Single(type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(field.IsPrivate);
        Assert.True(field.IsInitOnly);
        Assert.Equal(typeof(Task<RecordMetadata>), field.FieldType);
    }

    private static void AssertGetThrowsSynchronously(AsyncKafkaFuture<RecordMetadata> future)
    {
        // An Action, not a Func<Task>: the throw must come out of Get() itself. A Get() that
        // returned a faulted Task would not throw here, and would have assigned `returned`.
        Task<RecordMetadata>? returned = null;
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => { returned = future.Get(); });
        Assert.Equal(DefaultValueMessage, thrown.Message);
        Assert.Null(returned);
    }

    // Kept out of the test method itself (xUnit1030): the ConfigureAwait(false) is the subject here.
    private static async Task<RecordMetadata> AwaitGetWithoutContext(AsyncKafkaFuture<RecordMetadata> future) =>
        await future.Get().ConfigureAwait(false);

    private static Task<RecordMetadata> DeliveryIn(string state)
    {
        TaskCompletionSource<RecordMetadata> completion =
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
        switch (state)
        {
            case "pending":
                break;
            case "completed":
                completion.SetResult(NewMetadata());
                break;
            case "faulted":
                completion.SetException(new InvalidOperationException("delivery failed"));
                break;
            case "canceled":
                completion.SetCanceled();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, "unknown task state");
        }

        return completion.Task;
    }

    private static Task<RecordMetadata> Delivered(RecordMetadata metadata)
    {
        TaskCompletionSource<RecordMetadata> completion =
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
        completion.SetResult(metadata);
        return completion.Task;
    }

    private static RecordMetadata NewMetadata() => new RecordMetadata("future-topic", 3, 42L, 1_700_000_000_000L);

    // "<return type> <name>(<parameter types>)", with framework type names (Type.ToString()).
    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
