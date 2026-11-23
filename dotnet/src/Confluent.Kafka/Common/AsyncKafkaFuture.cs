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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common;

// Both async futures share this file, as ValueTask and ValueTask<TResult> do in the runtime.

/// <summary>
/// A future you wait for by awaiting the task its <see cref="Get()"/> returns: Java's <c>KafkaFuture&lt;T&gt;</c>, also
/// standing for the <c>Future&lt;RecordMetadata&gt;</c> that <c>IAsyncProducer.Send</c> returns;
/// <see cref="KafkaFuture{T}"/> is the sync counterpart. It is an <see cref="AsyncKafkaFuture"/>: the same future, seen
/// without its value.
/// </summary>
/// <remarks>
/// It is a type of its own rather than the bare <see cref="Task{TResult}"/>, so the two stages of a producer send stay
/// distinct values. Equality is identity. The binding synchronizes on the future object itself. Do not lock on a
/// future: a <c>lock (future)</c> can delay its completion and every later one of the same client, and a
/// <see cref="Get()"/> inside it can wait forever.
/// </remarks>
public abstract class AsyncKafkaFuture<T> : AsyncKafkaFuture, IEquatable<AsyncKafkaFuture<T>>
{
    private protected AsyncKafkaFuture()
    {
    }

    /// <summary>
    /// Java's <c>toCompletionStage</c>: a task, the same on every call, which user code cannot complete; it resolves
    /// with the value, faults with the future's failure itself, as Java's stage completes, or is canceled when the
    /// future is cancelled.
    /// </summary>
    public new abstract Task<T> ToCompletionStage();

    /// <summary>
    /// Java's <c>thenApply</c>: a future completed with <paramref name="function"/> applied to this future's value, or
    /// failed with this future's failure or the function's exception. The function runs on the thread that completes
    /// this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public abstract AsyncKafkaFuture<TR> ThenApply<TR>(KafkaFuture.BaseFunction<T, TR> function);

    /// <summary>
    /// Java's <c>whenComplete</c>: a future completed as this one is, once <paramref name="action"/> has run with the
    /// value (<c>default</c> when this future failed) and the failure (<see langword="null"/> when it succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public abstract AsyncKafkaFuture<T> WhenComplete(KafkaFuture.BiConsumer<T?, Exception?> action);

    /// <summary>
    /// The result's task, the same one on every call, to await as Java's <c>future.get()</c>: it resolves with the
    /// value, faults with <see cref="ExecutionException"/> around the future's failure, or is canceled when the future
    /// is cancelled. Never blocks.
    /// </summary>
    public new abstract Task<T> Get();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: a task completed as <see cref="Get()"/>'s is, or faulted with Java's
    /// <see cref="Confluent.Kafka.TimeoutException"/> when the future has not completed within
    /// <paramref name="timeout"/>. A zero or negative timeout checks once; <see cref="Timeout.InfiniteTimeSpan"/>
    /// waits as <see cref="Get()"/> does.
    /// </summary>
    public new abstract Task<T> Get(TimeSpan timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early by <paramref name="cancellationToken"/>: the returned task is canceled when the
    /// token fires first. The token ends only this wait, never the future.
    /// </summary>
    public new abstract Task<T> Get(CancellationToken cancellationToken);

    /// <inheritdoc cref="KafkaFuture{T}.GetNow"/>
    public abstract T GetNow(T valueIfAbsent);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> references are the same future.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture<T>? left, AsyncKafkaFuture<T>? right) =>
        ReferenceEquals(left, right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> references are different futures.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture<T>? left, AsyncKafkaFuture<T>? right) =>
        !ReferenceEquals(left, right);

    /// <inheritdoc/>
    public bool Equals(AsyncKafkaFuture<T>? other) => ReferenceEquals(this, other);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => ReferenceEquals(this, obj);

    /// <inheritdoc/>
    public override int GetHashCode() => RuntimeHelpers.GetHashCode(this);

    private protected sealed override Task CompletionStageTask() => ToCompletionStage();

    private protected sealed override Task GetTask() => Get();

    private protected sealed override Task GetTask(TimeSpan timeout) => Get(timeout);

    private protected sealed override Task GetTask(CancellationToken cancellationToken) => Get(cancellationToken);
}

/// <summary>
/// A future of no value that you wait for by awaiting the task its <see cref="Get()"/> returns: Java's
/// <c>KafkaFuture&lt;Void&gt;</c>; <see cref="KafkaFuture"/> is the sync counterpart. Every
/// <see cref="AsyncKafkaFuture{T}"/> is one. It also holds Java's static factories.
/// </summary>
/// <remarks>
/// Equality is identity. The binding synchronizes on the future object itself. Do not lock on a future: a
/// <c>lock (future)</c> can delay its completion and every later one of the same client, and a <see cref="Get()"/>
/// inside it can wait forever.
/// </remarks>
public abstract class AsyncKafkaFuture : IEquatable<AsyncKafkaFuture>
{
    // Only the binding's engines derive from the futures, so every future is one of them.
    private protected AsyncKafkaFuture()
    {
    }

    /// <inheritdoc cref="KafkaFuture.CompletedFuture{TU}"/>
    public static AsyncKafkaFuture<TU> CompletedFuture<TU>(TU value) =>
        FutureState.CompletedFuture(new AsyncKafkaFutureImpl<TU>(), value);

    /// <summary>
    /// Java's <c>allOf</c>: a future completed once every one of <paramref name="futures"/>, of any value types, has
    /// completed, failed with one of their failures when any of them failed; with no futures it is completed at once.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is or contains <see langword="null"/>.</exception>
    public static AsyncKafkaFuture AllOf(params AsyncKafkaFuture[] futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        var engines = new IKafkaFutureImpl[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            engines[i] = (IKafkaFutureImpl?)futures[i]
                ?? throw new ArgumentNullException(nameof(futures), FutureState.ContainsNullMessage);
        }

        return FutureState.AllOf(new AsyncKafkaFutureImpl<object?>(), engines);
    }

    /// <summary>
    /// Java's <c>allOf</c> for futures of one value type, such as an array of them.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is or contains <see langword="null"/>.</exception>
    public static AsyncKafkaFuture AllOf<T>(params AsyncKafkaFuture<T>[] futures) => AllOf((AsyncKafkaFuture[])futures);

    /// <summary>
    /// Java's <c>toCompletionStage</c>: a task, the same on every call, which user code cannot complete; it completes
    /// when the operation succeeds, faults with the future's failure itself, as Java's stage completes, or is canceled
    /// when the future is cancelled.
    /// </summary>
    public Task ToCompletionStage() => CompletionStageTask();

    /// <summary>
    /// Java's <c>thenApply</c> on a future of no value: a future completed with what <paramref name="function"/>
    /// returns once this future has succeeded, or failed with this future's failure or the function's exception. The
    /// function runs on the thread that completes this future, or inside this call if this future already holds its
    /// outcome.
    /// </summary>
    public abstract AsyncKafkaFuture<TR> ThenApply<TR>(Func<TR> function);

    /// <summary>
    /// Java's <c>whenComplete</c> on a future of no value: a future completed as this one is, once
    /// <paramref name="action"/> has run with the failure (<see langword="null"/> when this future succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public abstract AsyncKafkaFuture WhenComplete(Action<Exception?> action);

    /// <summary>
    /// Java's <c>cancel</c>: cancels this future if it is not yet done, and the futures that depend on it, never the
    /// operation behind it, and answers whether it is cancelled, by this call or by an earlier cancellation of its own.
    /// On a future that <c>IAsyncProducer.Send</c> returned it answers <see langword="false"/> and changes nothing.
    /// </summary>
    public abstract bool Cancel(bool mayInterruptIfRunning);

    /// <summary>
    /// The operation's task, the same one on every call, to await as Java's <c>future.get()</c>: it completes when the
    /// operation succeeds, faults with <see cref="ExecutionException"/> around the future's failure, or is canceled
    /// when the future is cancelled. Never blocks.
    /// </summary>
    public Task Get() => GetTask();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: a task completed as <see cref="Get()"/>'s is, or faulted with Java's
    /// <see cref="Confluent.Kafka.TimeoutException"/> when the future has not completed within
    /// <paramref name="timeout"/>. A zero or negative timeout checks once; <see cref="Timeout.InfiniteTimeSpan"/>
    /// waits as <see cref="Get()"/> does.
    /// </summary>
    public Task Get(TimeSpan timeout) => GetTask(timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early by <paramref name="cancellationToken"/>: the returned task is canceled when the
    /// token fires first. The token ends only this wait, never the future.
    /// </summary>
    public Task Get(CancellationToken cancellationToken) => GetTask(cancellationToken);

    /// <inheritdoc cref="KafkaFuture.GetNow"/>
    public abstract void GetNow();

    /// <inheritdoc cref="KafkaFuture.IsCancelled()"/>
    public abstract bool IsCancelled();

    /// <inheritdoc cref="KafkaFuture.IsCompletedExceptionally()"/>
    public abstract bool IsCompletedExceptionally();

    /// <inheritdoc cref="KafkaFuture.IsDone()"/>
    public abstract bool IsDone();

    /// <inheritdoc/>
    public abstract override string ToString();

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture"/> references are the same future.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture? left, AsyncKafkaFuture? right) => ReferenceEquals(left, right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture"/> references are different futures.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture? left, AsyncKafkaFuture? right) => !ReferenceEquals(left, right);

    /// <inheritdoc/>
    public bool Equals(AsyncKafkaFuture? other) => ReferenceEquals(this, other);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => ReferenceEquals(this, obj);

    /// <inheritdoc/>
    public override int GetHashCode() => RuntimeHelpers.GetHashCode(this);

    // The tasks behind the plain ToCompletionStage and Get forms: a future of a value hands out its own typed tasks.
    private protected abstract Task CompletionStageTask();

    private protected abstract Task GetTask();

    private protected abstract Task GetTask(TimeSpan timeout);

    private protected abstract Task GetTask(CancellationToken cancellationToken);
}
