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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

// Both async futures share this file, as ValueTask and ValueTask<TResult> do in the runtime.

/// <summary>
/// A future you wait for by awaiting the task its <see cref="Get()"/> returns: Java's <c>KafkaFuture&lt;T&gt;</c>, also
/// standing for the <c>Future&lt;RecordMetadata&gt;</c> that <c>IAsyncProducer.Send</c> returns;
/// <see cref="KafkaFuture{T}"/> is the sync counterpart. A <c>default</c> value is bound to no operation and throws
/// <see cref="InvalidOperationException"/>, synchronously, from every member but equality, conversion and
/// <see cref="ToString"/>.
/// </summary>
/// <remarks>
/// It is a struct over the task rather than the bare <see cref="Task{TResult}"/>, so the two stages of a producer send
/// stay distinct values. Equality is identity: two values are equal when they are copies of the same future, this one
/// and its <see cref="AsyncKafkaFuture"/> conversion included, and every <c>default</c> async future equals every
/// other, typed or untyped.
/// </remarks>
public readonly struct AsyncKafkaFuture<T> : IEquatable<AsyncKafkaFuture<T>>
{
    private readonly KafkaFutureImpl<T>? _impl;

    internal AsyncKafkaFuture(KafkaFutureImpl<T> impl)
    {
        _impl = impl;
    }

    /// <summary>
    /// Java's <c>toCompletionStage</c>: the same task as <see cref="Get()"/>, which user code cannot complete.
    /// </summary>
    public Task<T> ToCompletionStage() => Get();

    /// <summary>
    /// Java's <c>thenApply</c>: a future completed with <paramref name="function"/> applied to this future's value, or
    /// failed with this future's failure or the function's exception. The function runs on the thread that completes
    /// this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public AsyncKafkaFuture<TR> ThenApply<TR>(KafkaFuture.BaseFunction<T, TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return new AsyncKafkaFuture<TR>(Impl.ThenApply<TR>(function.Invoke));
    }

    /// <summary>
    /// Java's <c>whenComplete</c>: a future completed as this one is, once <paramref name="action"/> has run with the
    /// value (<c>default</c> when this future failed) and the failure (<see langword="null"/> when it succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public AsyncKafkaFuture<T> WhenComplete(KafkaFuture.BiConsumer<T?, Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return new AsyncKafkaFuture<T>(Impl.WhenComplete(action.Invoke));
    }

    /// <summary>
    /// Java's <c>cancel</c>: cancels this future if it is not yet done, and the futures that depend on it, never the
    /// operation behind it, and answers whether it is cancelled, by this call or by an earlier cancellation of its own.
    /// On a future returned by <c>IAsyncProducer.Send</c> it answers <see langword="false"/> and changes nothing.
    /// </summary>
    public bool Cancel(bool mayInterruptIfRunning) => Impl.Cancel(mayInterruptIfRunning);

    /// <summary>
    /// The result's task, the same one on every call, to await as Java's <c>future.get()</c>: it resolves with the
    /// value, faults with the operation's exception, or is canceled when the future is cancelled. Never blocks.
    /// </summary>
    public Task<T> Get() => Impl.GetAsync();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: a task completed as <see cref="Get()"/>'s is, or faulted with
    /// <see cref="TimeoutException"/> when the future has not completed within <paramref name="timeout"/>. A zero or
    /// negative timeout checks once; <see cref="Timeout.InfiniteTimeSpan"/> waits as <see cref="Get()"/> does.
    /// </summary>
    public Task<T> Get(TimeSpan timeout) => Impl.GetAsync(timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early by <paramref name="cancellationToken"/>: the returned task is canceled when the
    /// token fires first. The token ends only this wait, never the future.
    /// </summary>
    public Task<T> Get(CancellationToken cancellationToken) => Impl.GetAsync(cancellationToken);

    /// <inheritdoc cref="KafkaFuture{T}.GetNow"/>
    public T GetNow(T valueIfAbsent) => Impl.GetNow(valueIfAbsent);

    /// <inheritdoc cref="KafkaFuture{T}.IsCancelled"/>
    public bool IsCancelled => Impl.IsCancelled;

    /// <inheritdoc cref="KafkaFuture{T}.IsCompletedExceptionally"/>
    public bool IsCompletedExceptionally => Impl.IsCompletedExceptionally;

    /// <inheritdoc cref="KafkaFuture{T}.IsDone"/>
    public bool IsDone => Impl.IsDone;

    /// <inheritdoc/>
    public override string ToString() =>
        _impl is null ? base.ToString() ?? string.Empty : _impl.ToString();

    /// <summary>
    /// This future without its value, as Java's <c>KafkaFuture&lt;?&gt;</c>: the same future, so it completes when this
    /// one does, with the same failure. A <c>default</c> value converts to a <c>default</c> one.
    /// </summary>
    public static implicit operator AsyncKafkaFuture(AsyncKafkaFuture<T> future) => future.Erase();

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> values are equal — they are copies of the same future.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture<T> left, AsyncKafkaFuture<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> values are not equal — they are different futures.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture<T> left, AsyncKafkaFuture<T> right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(AsyncKafkaFuture<T> other) => ReferenceEquals(_impl, other._impl);

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is AsyncKafkaFuture<T> other ? Equals(other) : Erase().Equals(obj);

    /// <inheritdoc/>
    public override int GetHashCode() => _impl?.GetHashCode() ?? 0;

    internal AsyncKafkaFuture Erase() => _impl is not null ? new AsyncKafkaFuture(_impl) : default;

    internal KafkaFutureImpl<T> Impl =>
        _impl ?? throw new InvalidOperationException(AsyncKafkaFuture.DefaultValueMessage);
}

/// <summary>
/// A future of no value that you wait for by awaiting the task its <see cref="Get()"/> returns: Java's
/// <c>KafkaFuture&lt;Void&gt;</c>; <see cref="KafkaFuture"/> is the sync counterpart. It also holds Java's static
/// factories. A <c>default</c> value is bound to no operation and throws <see cref="InvalidOperationException"/>,
/// synchronously, from every instance member but equality and <see cref="ToString"/>.
/// </summary>
/// <remarks>
/// Equality is identity: two values are equal when they are copies of the same future, an
/// <see cref="AsyncKafkaFuture{T}"/> and its conversion to this type included, and every <c>default</c> async future
/// equals every other, typed or untyped.
/// </remarks>
public readonly struct AsyncKafkaFuture : IEquatable<AsyncKafkaFuture>
{
    internal const string DefaultValueMessage = "This AsyncKafkaFuture is a default value and is bound to no operation.";

    private readonly KafkaFutureImpl? _impl;

    internal AsyncKafkaFuture(KafkaFutureImpl impl)
    {
        _impl = impl;
    }

    /// <inheritdoc cref="KafkaFuture.CompletedFuture{TU}"/>
    public static AsyncKafkaFuture<TU> CompletedFuture<TU>(TU value) =>
        new AsyncKafkaFuture<TU>(KafkaFutureImpl.CompletedFuture(value));

    /// <summary>
    /// Java's <c>allOf</c>: a future completed once every one of <paramref name="futures"/>, of any value types, has
    /// completed, failed with one of their failures when any of them failed; with no futures it is completed at once.
    /// </summary>
    public static AsyncKafkaFuture AllOf(params AsyncKafkaFuture[] futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        var impls = new KafkaFutureImpl[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            impls[i] = futures[i].Impl;
        }

        return new AsyncKafkaFuture(KafkaFutureImpl.AllOf(impls));
    }

    /// <summary>
    /// Java's <c>allOf</c> for futures of one value type, such as an array of them.
    /// </summary>
    public static AsyncKafkaFuture AllOf<T>(params AsyncKafkaFuture<T>[] futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        var erased = new AsyncKafkaFuture[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            erased[i] = futures[i];
        }

        return AllOf(erased);
    }

    /// <summary>
    /// Java's <c>toCompletionStage</c>: the same task as <see cref="Get()"/>, which user code cannot complete.
    /// </summary>
    public Task ToCompletionStage() => Get();

    /// <summary>
    /// Java's <c>thenApply</c> on a future of no value: a future completed with what <paramref name="function"/>
    /// returns once this future has succeeded, or failed with this future's failure or the function's exception. The
    /// function runs on the thread that completes this future, or inside this call if this future already holds its
    /// outcome.
    /// </summary>
    public AsyncKafkaFuture<TR> ThenApply<TR>(Func<TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return new AsyncKafkaFuture<TR>(Impl.ThenApply(function));
    }

    /// <summary>
    /// Java's <c>whenComplete</c> on a future of no value: a future completed as this one is, once
    /// <paramref name="action"/> has run with the failure (<see langword="null"/> when this future succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public AsyncKafkaFuture WhenComplete(Action<Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return new AsyncKafkaFuture(Impl.WhenComplete(action));
    }

    /// <summary>
    /// Java's <c>cancel</c>: cancels this future if it is not yet done, and the futures that depend on it, never the
    /// operation behind it, and answers whether it is cancelled, by this call or by an earlier cancellation of its own.
    /// On a future that <c>IAsyncProducer.Send</c> returned, or one converted from it, it answers
    /// <see langword="false"/> and changes nothing.
    /// </summary>
    public bool Cancel(bool mayInterruptIfRunning) => Impl.Cancel(mayInterruptIfRunning);

    /// <summary>
    /// The operation's task, the same one on every call, to await as Java's <c>future.get()</c>: it completes when the
    /// operation succeeds, faults with its exception, or is canceled when the future is cancelled. Never blocks.
    /// </summary>
    public Task Get() => Impl.WaitAsync();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: a task completed as <see cref="Get()"/>'s is, or faulted with
    /// <see cref="TimeoutException"/> when the future has not completed within <paramref name="timeout"/>. A zero or
    /// negative timeout checks once; <see cref="Timeout.InfiniteTimeSpan"/> waits as <see cref="Get()"/> does.
    /// </summary>
    public Task Get(TimeSpan timeout) => Impl.WaitAsync(timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early by <paramref name="cancellationToken"/>: the returned task is canceled when the
    /// token fires first. The token ends only this wait, never the future.
    /// </summary>
    public Task Get(CancellationToken cancellationToken) => Impl.WaitAsync(cancellationToken);

    /// <inheritdoc cref="KafkaFuture.GetNow"/>
    public void GetNow() => Impl.GetNow();

    /// <inheritdoc cref="KafkaFuture.IsCancelled"/>
    public bool IsCancelled => Impl.IsCancelled;

    /// <inheritdoc cref="KafkaFuture.IsCompletedExceptionally"/>
    public bool IsCompletedExceptionally => Impl.IsCompletedExceptionally;

    /// <inheritdoc cref="KafkaFuture.IsDone"/>
    public bool IsDone => Impl.IsDone;

    /// <inheritdoc/>
    public override string ToString() =>
        _impl is null ? base.ToString() ?? string.Empty : _impl.ToString() ?? string.Empty;

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture"/> values are equal — they are copies of the same future.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture left, AsyncKafkaFuture right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture"/> values are not equal — they are different futures.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture left, AsyncKafkaFuture right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(AsyncKafkaFuture other) => ReferenceEquals(_impl, other._impl);

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is AsyncKafkaFuture other
            ? Equals(other)
            : obj is not null && obj.GetType() is { IsGenericType: true } type
                && type.GetGenericTypeDefinition() == typeof(AsyncKafkaFuture<>) && obj.Equals(this);

    /// <inheritdoc/>
    public override int GetHashCode() => _impl?.GetHashCode() ?? 0;

    internal KafkaFutureImpl Impl => _impl ?? throw new InvalidOperationException(DefaultValueMessage);
}
