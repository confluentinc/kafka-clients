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

namespace Confluent.Kafka.Common;

// Both sync futures share this file, as ValueTask and ValueTask<TResult> do in the runtime.

/// <summary>
/// A future you wait for by blocking: Java's <c>KafkaFuture&lt;T&gt;</c>, also standing for the
/// <c>Future&lt;RecordMetadata&gt;</c> that <c>IProducer.Send</c> returns; <see cref="AsyncKafkaFuture{T}"/> is the
/// async counterpart. It is a <see cref="KafkaFuture"/>: the same future, seen without its value.
/// </summary>
/// <remarks>
/// Equality is identity. <see cref="Get()"/> blocks the calling thread and needs no thread-pool thread. The binding
/// synchronizes on the future object itself. Do not lock on a future: a <c>lock (future)</c> can delay its completion
/// and every later one of the same client, and a <see cref="Get()"/> inside it can wait forever.
/// </remarks>
public abstract class KafkaFuture<T> : KafkaFuture
{
    private protected KafkaFuture()
    {
    }

    /// <summary>
    /// Java's <c>thenApply</c>: a future completed with <paramref name="function"/> applied to this future's value, or
    /// failed with this future's failure or the function's exception. The function runs on the thread that completes
    /// this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public abstract KafkaFuture<TR> ThenApply<TR>(BaseFunction<T, TR> function);

    /// <summary>
    /// Java's <c>whenComplete</c>: a future completed as this one is, once <paramref name="action"/> has run with the
    /// value (<c>default</c> when this future failed) and the failure (<see langword="null"/> when it succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public abstract KafkaFuture<T> WhenComplete(BiConsumer<T?, Exception?> action);

    /// <summary>
    /// Java's <c>Future.get()</c>: blocks until the operation completes, then returns its result. A failed future
    /// throws <see cref="ExecutionException"/>, whose <see cref="Exception.InnerException"/> is the failure; a
    /// cancelled one throws <see cref="OperationCanceledException"/>. Inside a delivery callback, on the record's own
    /// future, it never returns, as in Java.
    /// </summary>
    public new abstract T Get();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: <see cref="Get()"/>, throwing Java's
    /// <see cref="Confluent.Kafka.TimeoutException"/> when the future has not completed within
    /// <paramref name="timeout"/>; a zero or negative timeout checks once, and <see cref="Timeout.InfiniteTimeSpan"/>
    /// waits as <see cref="Get()"/> does.
    /// </summary>
    public new abstract T Get(TimeSpan timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early with <see cref="OperationCanceledException"/> when
    /// <paramref name="cancellationToken"/> fires first; the token ends only this wait, never the future.
    /// </summary>
    public new abstract T Get(CancellationToken cancellationToken);

    /// <summary>
    /// Java's <c>getNow</c>: the future's value when it has completed, else <paramref name="valueIfAbsent"/>; a failed
    /// future throws <see cref="ExecutionException"/> around its failure, and a cancelled one
    /// <see cref="OperationCanceledException"/>. Never blocks.
    /// </summary>
    public abstract T GetNow(T valueIfAbsent);

    private protected sealed override void WaitForGet() => _ = Get();

    private protected sealed override void WaitForGet(TimeSpan timeout) => _ = Get(timeout);

    private protected sealed override void WaitForGet(CancellationToken cancellationToken) => _ = Get(cancellationToken);
}

/// <summary>
/// A future of no value that you wait for by blocking: Java's <c>KafkaFuture&lt;Void&gt;</c>;
/// <see cref="AsyncKafkaFuture"/> is the async counterpart. Every <see cref="KafkaFuture{T}"/> is one. It also holds
/// Java's static factories and the two delegate types the futures take.
/// </summary>
/// <remarks>
/// Equality is identity. <see cref="Get()"/> blocks the calling thread and needs no thread-pool thread. The binding
/// synchronizes on the future object itself. Do not lock on a future: a <c>lock (future)</c> can delay its completion
/// and every later one of the same client, and a <see cref="Get()"/> inside it can wait forever.
/// </remarks>
public abstract class KafkaFuture
{
    // Only the binding's engines derive from the futures, so every future is one of them.
    private protected KafkaFuture()
    {
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
        FutureState.CompletedFuture(new KafkaFutureImpl<TU>(), value);

    /// <summary>
    /// Java's <c>allOf</c>: a future completed once every one of <paramref name="futures"/>, of any value types, has
    /// completed, failed with one of their failures when any of them failed; with no futures it is completed at once.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is or contains <see langword="null"/>.</exception>
    public static KafkaFuture AllOf(params KafkaFuture[] futures)
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

        return FutureState.AllOf(new KafkaFutureImpl<object?>(), engines);
    }

    /// <summary>
    /// Java's <c>allOf</c> for futures of one value type, such as an array of them.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is or contains <see langword="null"/>.</exception>
    public static KafkaFuture AllOf<T>(params KafkaFuture<T>[] futures) => AllOf((KafkaFuture[])futures);

    /// <summary>
    /// Java's <c>thenApply</c> on a future of no value: a future completed with what <paramref name="function"/>
    /// returns once this future has succeeded, or failed with this future's failure or the function's exception. The
    /// function runs on the thread that completes this future, or inside this call if this future already holds its
    /// outcome.
    /// </summary>
    public abstract KafkaFuture<TR> ThenApply<TR>(Func<TR> function);

    /// <summary>
    /// Java's <c>whenComplete</c> on a future of no value: a future completed as this one is, once
    /// <paramref name="action"/> has run with the failure (<see langword="null"/> when this future succeeded); when
    /// this future succeeded and the action throws, the returned future fails with that exception. The action runs on
    /// the thread that completes this future, or inside this call if this future already holds its outcome.
    /// </summary>
    public abstract KafkaFuture WhenComplete(Action<Exception?> action);

    /// <summary>
    /// Java's <c>cancel</c>: cancels this future if it is not yet done, and the futures that depend on it, never the
    /// operation behind it, and answers whether it is cancelled, by this call or by an earlier cancellation of its own.
    /// On a future that <c>IProducer.Send</c> returned it answers <see langword="false"/> and changes nothing.
    /// </summary>
    public abstract bool Cancel(bool mayInterruptIfRunning);

    /// <summary>
    /// Java's <c>Future.get()</c> on a future of no value: blocks until the operation completes, then returns. A
    /// failed future throws <see cref="ExecutionException"/>, whose <see cref="Exception.InnerException"/> is the
    /// failure; a cancelled one throws <see cref="OperationCanceledException"/>.
    /// </summary>
    public void Get() => WaitForGet();

    /// <summary>
    /// Java's <c>get(long, TimeUnit)</c>: <see cref="Get()"/>, throwing Java's
    /// <see cref="Confluent.Kafka.TimeoutException"/> when the future has not completed within
    /// <paramref name="timeout"/>; a zero or negative timeout checks once, and <see cref="Timeout.InfiniteTimeSpan"/>
    /// waits as <see cref="Get()"/> does.
    /// </summary>
    public void Get(TimeSpan timeout) => WaitForGet(timeout);

    /// <summary>
    /// <see cref="Get()"/>, ended early with <see cref="OperationCanceledException"/> when
    /// <paramref name="cancellationToken"/> fires first; the token ends only this wait, never the future.
    /// </summary>
    public void Get(CancellationToken cancellationToken) => WaitForGet(cancellationToken);

    /// <summary>
    /// Java's <c>getNow(null)</c> on a future of no value: returns when the future is pending or has succeeded; a
    /// failed future throws <see cref="ExecutionException"/> around its failure, and a cancelled one
    /// <see cref="OperationCanceledException"/>. Never blocks.
    /// </summary>
    public abstract void GetNow();

    /// <summary>
    /// Java's <c>isCancelled()</c>: whether the future was cancelled, by <see cref="Cancel"/> or by the cancellation of
    /// the future it depends on. Never blocks.
    /// </summary>
    public abstract bool IsCancelled();

    /// <summary>
    /// Java's <c>isCompletedExceptionally()</c>: whether the future failed or was cancelled. Never blocks.
    /// </summary>
    public abstract bool IsCompletedExceptionally();

    /// <summary>
    /// Java <c>Future.isDone()</c>: whether the operation has completed, successfully or not. Never blocks, and runs no
    /// dependant or <c>WhenComplete</c> action.
    /// </summary>
    public abstract bool IsDone();

    /// <inheritdoc/>
    public abstract override string ToString();

    // The wait behind the plain Get forms: a future of a value waits through its own typed Get.
    private protected abstract void WaitForGet();

    private protected abstract void WaitForGet(TimeSpan timeout);

    private protected abstract void WaitForGet(CancellationToken cancellationToken);
}
