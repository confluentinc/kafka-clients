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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

// Both sync futures share this file, as ValueTask and ValueTask<TResult> do in the runtime.

/// <summary>
/// A future you wait for by blocking: Java's <c>KafkaFuture&lt;T&gt;</c>, also standing for the
/// <c>Future&lt;RecordMetadata&gt;</c> that <c>IProducer.Send</c> returns; <see cref="AsyncKafkaFuture{T}"/> is the
/// async counterpart. A <c>default</c> value is bound to no operation and throws <see cref="InvalidOperationException"/>
/// from every member but equality, conversion and <see cref="ToString"/>.
/// </summary>
/// <remarks>
/// Equality is identity: two values are equal when they are copies of the same future, this one and its
/// <see cref="KafkaFuture"/> conversion included, and every <c>default</c> sync future equals every other, typed or
/// untyped.
/// </remarks>
public readonly struct KafkaFuture<T> : IEquatable<KafkaFuture<T>>
{
    private readonly KafkaFutureImpl<T>? _impl;

    private readonly SyncFutureSource<T>? _source;

    internal KafkaFuture(KafkaFutureImpl<T> impl)
    {
        _impl = impl;
        _source = null;
    }

    // Holds a managed completion, never a native handle, so a copied or discarded value leaks nothing.
    internal KafkaFuture(SyncFutureSource<T> source)
    {
        _impl = null;
        _source = source;
    }

    /// <summary>
    /// Java's <c>thenApply</c>: a future completed with <paramref name="function"/> applied to this future's value, or
    /// failed with this future's failure or the function's exception. The function runs on the thread that completes
    /// this future, or inside this call if this future already holds its outcome. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public KafkaFuture<TR> ThenApply<TR>(KafkaFuture.BaseFunction<T, TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return new KafkaFuture<TR>(Impl(nameof(ThenApply)).ThenApply<TR>(function.Invoke));
    }

    /// <summary>
    /// Java's <c>whenComplete</c>: a future completed as this one is, once <paramref name="action"/> has run with the
    /// value (<c>default</c> when this future failed) and the failure (<see langword="null"/> when it succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public KafkaFuture<T> WhenComplete(KafkaFuture.BiConsumer<T?, Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return new KafkaFuture<T>(Impl(nameof(WhenComplete)).WhenComplete(action.Invoke));
    }

    /// <summary>
    /// Java's <c>cancel</c>: cancels this future if it is not yet done, and the futures that depend on it, never the
    /// operation behind it, and answers whether it is cancelled, by this call or by an earlier cancellation of its own.
    /// On a future returned by <c>IProducer.Send</c> or an <c>IAdmin</c> result it answers <see langword="false"/> and
    /// changes nothing.
    /// </summary>
    public bool Cancel(bool mayInterruptIfRunning) => Impl().Cancel(mayInterruptIfRunning);

    /// <summary>
    /// Java's <c>Future.get()</c>: blocks until the operation completes, then returns its result or rethrows its
    /// exception, unwrapped. Inside a delivery callback, on the record's own future, it never returns, as in Java.
    /// </summary>
    public T Get() => _impl is null ? Source.Get() : _impl.Get();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: <see cref="Get()"/>, throwing <see cref="TimeoutException"/> when the future
    /// has not completed within <paramref name="timeout"/>; a zero or negative timeout checks once, and
    /// <see cref="Timeout.InfiniteTimeSpan"/> waits as <see cref="Get()"/> does. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public T Get(TimeSpan timeout) => Impl(nameof(Get)).Get(timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early with <see cref="OperationCanceledException"/> when
    /// <paramref name="cancellationToken"/> fires first; the token ends only this wait, never the future. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public T Get(CancellationToken cancellationToken) => Impl(nameof(Get)).Get(cancellationToken);

    /// <summary>
    /// Java's <c>getNow</c>: the future's value when it has completed, else <paramref name="valueIfAbsent"/>; rethrows
    /// the exception of a failed or cancelled future. Never blocks.
    /// </summary>
    public T GetNow(T valueIfAbsent) => Impl().GetNow(valueIfAbsent);

    /// <summary>
    /// Java's <c>isCancelled()</c>: whether the future was cancelled, by <see cref="Cancel"/> or by the cancellation of
    /// the future it depends on. Never blocks.
    /// </summary>
    public bool IsCancelled => Impl().IsCancelled;

    /// <summary>
    /// Java's <c>isCompletedExceptionally()</c>: whether the future failed or was cancelled. Never blocks.
    /// </summary>
    public bool IsCompletedExceptionally => Impl().IsCompletedExceptionally;

    /// <summary>
    /// Java <c>Future.isDone()</c>: whether the operation has completed, successfully or not. Never blocks, and runs no
    /// dependant or <c>WhenComplete</c> action.
    /// </summary>
    public bool IsDone => _impl?.IsDone ?? Source.IsDone;

    /// <inheritdoc/>
    public override string ToString() =>
        _impl is null && _source is null ? base.ToString() ?? string.Empty : Impl().ToString();

    /// <summary>
    /// This future without its value, as Java's <c>KafkaFuture&lt;?&gt;</c>: the same future, so it completes when this
    /// one does, with the same failure. A <c>default</c> value converts to a <c>default</c> one.
    /// </summary>
    public static implicit operator KafkaFuture(KafkaFuture<T> future) => future.Erase();

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture{T}"/> values are equal — they are copies of the same future.
    /// </summary>
    public static bool operator ==(KafkaFuture<T> left, KafkaFuture<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture{T}"/> values are not equal — they are different futures.
    /// </summary>
    public static bool operator !=(KafkaFuture<T> left, KafkaFuture<T> right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(KafkaFuture<T> other) =>
        ReferenceEquals(_impl, other._impl) && ReferenceEquals(_source, other._source);

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is KafkaFuture<T> other ? Equals(other) : Erase().Equals(obj);

    /// <inheritdoc/>
    public override int GetHashCode() => ((object?)_impl ?? _source)?.GetHashCode() ?? 0;

    internal KafkaFuture Erase() =>
        _impl is not null ? new KafkaFuture(_impl) : _source is not null ? new KafkaFuture(_source) : default;

    internal SyncFutureSource<T> Source => _source ?? throw new InvalidOperationException(KafkaFuture.DefaultValueMessage);

    // A pending SyncFutureSource has no completion signal or timed wait: a member that needs one throws
    // NotSupportedException, and every other read takes a snapshot of the source that cannot be cancelled.
    private KafkaFutureImpl<T> Impl(string? member = null)
    {
        if (_impl is not null)
        {
            return _impl;
        }

        SyncFutureSource<T> source = Source;
        bool done = source.IsDone;
        if (!done && member is not null)
        {
            throw new NotSupportedException(KafkaFuture.UnsupportedMessage(member));
        }

        var snapshot = new KafkaFutureImpl<T>(mayCancel: false);
        if (done)
        {
            try
            {
                snapshot.Complete(source.Get());
            }
            catch (Exception error)
            {
                snapshot.CompleteWithError(error);
            }
        }

        return snapshot;
    }
}

/// <summary>
/// A future of no value that you wait for by blocking: Java's <c>KafkaFuture&lt;Void&gt;</c>;
/// <see cref="AsyncKafkaFuture"/> is the async counterpart. It also holds Java's static factories and the two delegate
/// types the futures take. A <c>default</c> value is bound to no operation and throws
/// <see cref="InvalidOperationException"/> from every instance member but equality and <see cref="ToString"/>.
/// </summary>
/// <remarks>
/// Equality is identity: two values are equal when they are copies of the same future, a <see cref="KafkaFuture{T}"/>
/// and its conversion to this type included, and every <c>default</c> sync future equals every other, typed or untyped.
/// </remarks>
public readonly struct KafkaFuture : IEquatable<KafkaFuture>
{
    internal const string DefaultValueMessage = "This KafkaFuture is a default value and is bound to no operation.";

    private readonly KafkaFutureImpl? _impl;

    private readonly SyncFutureSource? _source;

    internal KafkaFuture(KafkaFutureImpl impl)
    {
        _impl = impl;
        _source = null;
    }

    internal KafkaFuture(SyncFutureSource source)
    {
        _impl = null;
        _source = source;
    }

    /// <summary>
    /// Java's <c>KafkaFuture.BaseFunction</c>: the function <c>ThenApply</c> applies to a future's value.
    /// </summary>
    public delegate TB BaseFunction<in TA, out TB>(TA a);

    /// <summary>
    /// Java's <c>KafkaFuture.BiConsumer</c>: the action <c>WhenComplete</c> runs with a future's value and failure.
    /// </summary>
    public delegate void BiConsumer<in TA, in TB>(TA a, TB b);

    /// <summary>
    /// Java's <c>completedFuture</c>: a future already completed with <paramref name="value"/>.
    /// </summary>
    public static KafkaFuture<TU> CompletedFuture<TU>(TU value) =>
        new KafkaFuture<TU>(KafkaFutureImpl.CompletedFuture(value));

    /// <summary>
    /// Java's <c>allOf</c>: a future completed once every one of <paramref name="futures"/>, of any value types, has
    /// completed, failed with one of their failures when any of them failed; with no futures it is completed at once.
    /// Throws <see cref="NotSupportedException"/> when one of them is pending and was returned by an
    /// <c>IAdmin</c> result.
    /// </summary>
    public static KafkaFuture AllOf(params KafkaFuture[] futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        var impls = new KafkaFutureImpl[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            impls[i] = futures[i].Impl(nameof(AllOf));
        }

        return new KafkaFuture(KafkaFutureImpl.AllOf(impls));
    }

    /// <summary>
    /// Java's <c>allOf</c> for futures of one value type, such as an array of them. Throws
    /// <see cref="NotSupportedException"/> when one of them is pending and was returned by an <c>IAdmin</c> result.
    /// </summary>
    public static KafkaFuture AllOf<T>(params KafkaFuture<T>[] futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        var erased = new KafkaFuture[futures.Length];
        for (int i = 0; i < futures.Length; i++)
        {
            erased[i] = futures[i];
        }

        return AllOf(erased);
    }

    /// <summary>
    /// Java's <c>thenApply</c> on a future of no value: a future completed with what <paramref name="function"/>
    /// returns once this future has succeeded, or failed with this future's failure or the function's exception. The
    /// function runs on the thread that completes this future, or inside this call if this future already holds its
    /// outcome. Throws <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public KafkaFuture<TR> ThenApply<TR>(Func<TR> function)
    {
        if (function is null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        return new KafkaFuture<TR>(Impl(nameof(ThenApply)).ThenApply(function));
    }

    /// <summary>
    /// Java's <c>whenComplete</c> on a future of no value: a future completed as this one is, once
    /// <paramref name="action"/> has run with the failure (<see langword="null"/> when this future succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public KafkaFuture WhenComplete(Action<Exception?> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return new KafkaFuture(Impl(nameof(WhenComplete)).WhenComplete(action));
    }

    /// <summary>
    /// Java's <c>cancel</c>: cancels this future if it is not yet done, and the futures that depend on it, never the
    /// operation behind it, and answers whether it is cancelled, by this call or by an earlier cancellation of its own.
    /// On a future that <c>IProducer.Send</c> or an <c>IAdmin</c> result returned, or one converted from it, it answers
    /// <see langword="false"/> and changes nothing.
    /// </summary>
    public bool Cancel(bool mayInterruptIfRunning) => Impl().Cancel(mayInterruptIfRunning);

    /// <summary>
    /// Java's <c>Future.get()</c> on a future of no value: blocks until the operation completes, then returns or
    /// rethrows its exception, unwrapped.
    /// </summary>
    public void Get()
    {
        if (_impl is null)
        {
            Source.Wait();
        }
        else
        {
            _impl.Wait();
        }
    }

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: <see cref="Get()"/>, throwing <see cref="TimeoutException"/> when the future
    /// has not completed within <paramref name="timeout"/>; a zero or negative timeout checks once, and
    /// <see cref="Timeout.InfiniteTimeSpan"/> waits as <see cref="Get()"/> does. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public void Get(TimeSpan timeout) => Impl(nameof(Get)).Wait(timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early with <see cref="OperationCanceledException"/> when
    /// <paramref name="cancellationToken"/> fires first; the token ends only this wait, never the future. Throws
    /// <see cref="NotSupportedException"/> on a pending future returned by an <c>IAdmin</c> result.
    /// </summary>
    public void Get(CancellationToken cancellationToken) => Impl(nameof(Get)).Wait(cancellationToken);

    /// <summary>
    /// Java's <c>getNow(null)</c> on a future of no value: returns when the future is pending or has succeeded, and
    /// rethrows the exception of a failed or cancelled future. Never blocks.
    /// </summary>
    public void GetNow() => Impl().GetNow();

    /// <summary>
    /// Java's <c>isCancelled()</c>: whether the future was cancelled, by <see cref="Cancel"/> or by the cancellation of
    /// the future it depends on. Never blocks.
    /// </summary>
    public bool IsCancelled => Impl().IsCancelled;

    /// <summary>
    /// Java's <c>isCompletedExceptionally()</c>: whether the future failed or was cancelled. Never blocks.
    /// </summary>
    public bool IsCompletedExceptionally => Impl().IsCompletedExceptionally;

    /// <summary>
    /// Java <c>Future.isDone()</c>: whether the operation has completed, successfully or not. Never blocks, and runs no
    /// dependant or <c>WhenComplete</c> action.
    /// </summary>
    public bool IsDone => _impl?.IsDone ?? Source.IsDone;

    /// <inheritdoc/>
    public override string ToString() =>
        _impl is null && _source is null ? base.ToString() ?? string.Empty : Impl().ToString() ?? string.Empty;

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture"/> values are equal — they are copies of the same future.
    /// </summary>
    public static bool operator ==(KafkaFuture left, KafkaFuture right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture"/> values are not equal — they are different futures.
    /// </summary>
    public static bool operator !=(KafkaFuture left, KafkaFuture right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(KafkaFuture other) =>
        ReferenceEquals(_impl, other._impl) && ReferenceEquals(_source, other._source);

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is KafkaFuture other
            ? Equals(other)
            : obj is not null && obj.GetType() is { IsGenericType: true } type
                && type.GetGenericTypeDefinition() == typeof(KafkaFuture<>) && obj.Equals(this);

    /// <inheritdoc/>
    public override int GetHashCode() => ((object?)_impl ?? _source)?.GetHashCode() ?? 0;

    internal SyncFutureSource Source => _source ?? throw new InvalidOperationException(DefaultValueMessage);

    internal static string UnsupportedMessage(string member) =>
        "KafkaFuture." + member + " is not supported on a pending future returned by an IAdmin result.";

    private KafkaFutureImpl Impl(string? member = null)
    {
        if (_impl is not null)
        {
            return _impl;
        }

        SyncFutureSource source = Source;
        bool done = source.IsDone;
        if (!done && member is not null)
        {
            throw new NotSupportedException(UnsupportedMessage(member));
        }

        var snapshot = new KafkaFutureImpl<object?>(mayCancel: false);
        if (done)
        {
            try
            {
                source.Wait();
                snapshot.Complete(null);
            }
            catch (Exception error)
            {
                snapshot.CompleteWithError(error);
            }
        }

        return snapshot;
    }
}
