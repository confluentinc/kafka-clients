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
/// The public non-generic <see cref="AsyncKafkaFuture"/> (M15/P14 S1, PLAN §2.3) — Java's <c>KafkaFuture&lt;Void&gt;</c>
/// on the async side — built through its <c>internal</c> constructor over a <see cref="Task"/>, mirroring
/// <see cref="PublicAsyncKafkaFutureTests"/> row for row: the synchronous default-value guard, <see cref="AsyncKafkaFuture.Get"/>'s
/// same-instance contract, reference-identity equality, the erasure from <see cref="AsyncKafkaFuture{T}"/> (the same
/// task, never a new one), and the approved member set (no <c>IsDone</c>, no <c>GetAwaiter</c>: P3.6 D5/D6).
/// </summary>
public sealed class PublicNonGenericAsyncKafkaFutureTests
{
    // M15/P14 S1, verbatim — the same text as AsyncKafkaFuture<T>'s.
    private const string DefaultValueMessage =
        "This AsyncKafkaFuture is a default value and is bound to no operation; only IAsyncProducer.Send and the "
        + "IAsyncAdmin result types return a usable one.";

    [Fact]
    public void Default_Get_ThrowsInvalidOperationException_Synchronously_WithItsMessage()
    {
        AssertGetThrowsSynchronously(default(AsyncKafkaFuture));
        AssertGetThrowsSynchronously(new AsyncKafkaFuture());

        // The text is AsyncKafkaFuture<T>'s own, so the two default guards cannot drift apart.
        InvalidOperationException typed =
            Assert.Throws<InvalidOperationException>(() => { _ = default(AsyncKafkaFuture<RecordMetadata>).Get(); });
        Assert.Equal(DefaultValueMessage, typed.Message);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("completed")]
    [InlineData("faulted")]
    [InlineData("canceled")]
    public void Get_ReturnsTheWrappedTask_TheSameInstanceOnEveryCall(string state)
    {
        Task task = TaskIn(state);
        AsyncKafkaFuture future = new AsyncKafkaFuture(task);

        Task first = future.Get();
        Task second = future.Get();

        Assert.Same(task, first);
        Assert.Same(first, second);

        // Observe a faulted task's exception so it is never reported as unobserved by the finalizer.
        _ = task.Exception;
    }

    [Fact]
    public void Equality_IsReferenceIdentityOfTheTask()
    {
        // Same task → equal, ==, same hash.
        Task task = Task.CompletedTask;
        AsyncKafkaFuture a = new AsyncKafkaFuture(task);
        AsyncKafkaFuture sameTask = new AsyncKafkaFuture(task);
        Assert.True(a.Equals(sameTask));
        Assert.True(a == sameTask);
        Assert.False(a != sameTask);
        Assert.Equal(a.GetHashCode(), sameTask.GetHashCode());

        // Different tasks, both completed → not equal: identity, never the outcome.
        Task firstTask = TaskIn("completed");
        Task secondTask = TaskIn("completed");
        Assert.NotSame(firstTask, secondTask);
        AsyncKafkaFuture x = new AsyncKafkaFuture(firstTask);
        AsyncKafkaFuture y = new AsyncKafkaFuture(secondTask);
        Assert.False(x.Equals(y));
        Assert.False(x == y);
        Assert.True(x != y);

        // default == default; default != non-default (both directions).
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

        // Equals(object): a boxed equal other, a boxed different one, the raw task itself, a typed future over the same
        // task (a different type, so never equal), a string, and null.
        Task<bool> typedTask = Task.FromResult(true);
        object boxedEqual = sameTask;
        object boxedDifferent = y;
        object rawTask = task;
        object typedOverTheSameTask = new AsyncKafkaFuture<bool>(typedTask);
        Assert.True(a.Equals(boxedEqual));
        Assert.False(x.Equals(boxedDifferent));
        Assert.False(a.Equals(rawTask));
        Assert.False(new AsyncKafkaFuture(typedTask).Equals(typedOverTheSameTask));
        Assert.False(a.Equals("not a future"));
        Assert.False(a.Equals(null));
    }

    [Fact]
    public async Task Erasure_WrapsTheSameTask_AndCompletesExactlyWhenTheTypedFutureDoes()
    {
        TaskCompletionSource<bool> completion =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        AsyncKafkaFuture<bool> typed = new AsyncKafkaFuture<bool>(completion.Task);

        AsyncKafkaFuture first = typed.Erase();
        AsyncKafkaFuture second = typed.Erase();

        // The very task the typed future hands out — no new task, no continuation — so two erasures are equal, equal to
        // a future built over that task, and share its hash.
        Assert.Same(typed.Get(), first.Get());
        Assert.True(first == second);
        Assert.True(first == new AsyncKafkaFuture(completion.Task));
        Assert.Equal(typed.GetHashCode(), first.GetHashCode());
        Assert.True(first != new AsyncKafkaFuture<bool>(Task.FromResult(true)).Erase());

        // It completes exactly when the typed one does: it is that task.
        Assert.False(first.Get().IsCompleted);
        completion.SetResult(true);
        await TestTimeout.Run(() => first.Get(), BlockingCall.Deadline);
        Assert.True(first.Get().IsCompleted);
        Assert.True(typed.Get().IsCompleted);

        // A default typed future erases to a default one.
        Assert.True(default(AsyncKafkaFuture<bool>).Erase() == default(AsyncKafkaFuture));
    }

    [Fact]
    public void Shape_IsAPublicReadonlyStruct_WithOnlyTheApprovedMembers()
    {
        Type type = typeof(AsyncKafkaFuture);

        Assert.True(type.IsValueType);
        Assert.True(type.IsPublic);
        Assert.False(type.IsGenericType);
        Assert.Equal("Confluent.Kafka", type.Namespace);
        Assert.Contains(
            type.CustomAttributes,
            attribute => attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        // Exactly one interface: IEquatable<self>, as AsyncKafkaFuture<T>.
        Assert.Equal(new[] { typeof(IEquatable<AsyncKafkaFuture>) }, type.GetInterfaces());

        // Declared public instance methods: Get + the equality three. No GetAwaiter / ConfigureAwait (P3.6 D5), no
        // IsDone / Get(TimeSpan) / IsCancelled / Cancel (P3.6 D6), no conversion, no ToString override.
        string[] instanceMethods = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(Signature)
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[]
            {
                "System.Boolean Equals(Confluent.Kafka.AsyncKafkaFuture)",
                "System.Boolean Equals(System.Object)",
                "System.Int32 GetHashCode()",
                "System.Threading.Tasks.Task Get()",
            },
            instanceMethods);

        const BindingFlags AnyInstance = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Null(type.GetMethod("GetAwaiter", AnyInstance));
        Assert.Null(type.GetMethod("ConfigureAwait", AnyInstance));
        Assert.Null(type.GetProperty("IsDone", AnyInstance));
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
                "System.Boolean op_Equality(Confluent.Kafka.AsyncKafkaFuture, Confluent.Kafka.AsyncKafkaFuture)",
                "System.Boolean op_Inequality(Confluent.Kafka.AsyncKafkaFuture, Confluent.Kafka.AsyncKafkaFuture)",
            },
            staticMethods);

        // No public constructor; the one internal constructor takes the task.
        Assert.Empty(type.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        ConstructorInfo constructor = Assert.Single(type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(constructor.IsAssembly);
        Assert.Equal(
            new[] { typeof(Task) },
            constructor.GetParameters().Select(parameter => parameter.ParameterType).ToArray());

        // No public fields or properties; the one private field is a readonly Task.
        Assert.Empty(type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static));
        FieldInfo field = Assert.Single(type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(field.IsPrivate);
        Assert.True(field.IsInitOnly);
        Assert.Equal(typeof(Task), field.FieldType);
    }

    private static void AssertGetThrowsSynchronously(AsyncKafkaFuture future)
    {
        // An Action, not a Func<Task>: the throw must come out of Get() itself. A Get() that returned a faulted Task
        // would not throw here, and would have assigned `returned`.
        Task? returned = null;
        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(() => { returned = future.Get(); });
        Assert.Equal(DefaultValueMessage, thrown.Message);
        Assert.Null(returned);
    }

    private static Task TaskIn(string state)
    {
        TaskCompletionSource<bool> completion =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        switch (state)
        {
            case "pending":
                break;
            case "completed":
                completion.SetResult(true);
                break;
            case "faulted":
                completion.SetException(new InvalidOperationException("the operation failed"));
                break;
            case "canceled":
                completion.SetCanceled();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, "unknown task state");
        }

        return completion.Task;
    }

    // "<return type> <name>(<parameter types>)", with framework type names (Type.ToString()).
    private static string Signature(MethodInfo method) =>
        method.ReturnType + " " + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.ToString()))
        + ")";
}
