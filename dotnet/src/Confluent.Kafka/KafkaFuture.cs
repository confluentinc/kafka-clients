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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

// Both sync futures live in this file, side by side, as ValueTask and ValueTask<TResult> do in the runtime: the
// non-generic one is KafkaFuture<T> without a value, and its shape and equality mirror the generic one member for
// member (M15/P14 S1).

/// <summary>
/// The handle on an operation's result that you wait for by <b>blocking</b> — Java's
/// <c>org.apache.kafka.common.KafkaFuture&lt;T&gt;</c>, and the <c>java.util.concurrent.Future&lt;RecordMetadata&gt;</c>
/// that Java's <c>Producer.send</c> returns. <see cref="Get()"/> blocks until the operation completes and returns its
/// result or rethrows its failure (Java's <c>get()</c>). Only <c>IProducer.Send</c> and the <c>IAdmin</c> result types
/// return a usable one; <see cref="AsyncKafkaFuture{T}"/> is the async counterpart.
/// </summary>
/// <typeparam name="T">The operation's result: <see cref="RecordMetadata"/> for a producer send.</typeparam>
/// <remarks>
/// <list type="bullet">
/// <item><b>Get blocks the calling thread</b> on a managed latch — not a <c>Task</c>, so this is not
/// sync-over-async, and no thread-pool thread is involved. Call it any number of times from any number of threads;
/// every call returns the same <typeparamref name="T"/> or rethrows the same exception.</item>
/// <item><b>IsDone never blocks.</b> <see cref="IsDone"/> is <see langword="true"/> once the operation has completed,
/// successfully or not (Java's <c>isDone()</c>); <see cref="Get()"/> then returns or throws without waiting.</item>
/// <item><b>Failure.</b> <see cref="Get()"/> rethrows the operation's exception itself (a <see cref="KafkaException"/>
/// for a Kafka failure). Java wraps it in <c>ExecutionException</c>; .NET has no such type.</item>
/// <item><b>Fire-and-forget is fine.</b> Discarding the value leaks nothing: it holds no native resource.</item>
/// <item><b>default.</b> A <c>default</c> value is bound to no operation: <see cref="Get()"/> and
/// <see cref="IsDone"/> throw <see cref="InvalidOperationException"/>.</item>
/// <item><b>Equality.</b> Two values are equal exactly when they are copies of the same future — the identity of
/// what they wait on, never the result.</item>
/// <item><b>A producer send's future</b> (<see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/>):
/// <c>Send</c> does not wait for delivery — it returns once the core has accepted the record (it blocks only while the
/// core waits for metadata or for <c>buffer.memory</c>, up to <c>max.block.ms</c> — Java's <c>send</c> blocking), and
/// the key and value buffers are reusable on return. With <c>Send(record, callback)</c> the callback runs on the
/// producer's send-completion thread, before <see cref="Get()"/> returns (Java's ordering). Inside a delivery
/// callback, <see cref="Get()"/> for a not-yet-completed send of the same producer throws
/// <see cref="InvalidOperationException"/> instead of deadlocking. One producer completes its sends in the order
/// <c>Send</c> returned, so a slow partition delays later completions on the same producer (M11/P4.2 §D1).</item>
/// <item><b>Naming.</b> The name is Java's admin future type, <c>org.apache.kafka.common.KafkaFuture</c>, and pairs
/// with <see cref="AsyncKafkaFuture{T}"/>; for the producer it stands for <c>java.util.concurrent.Future</c>, which
/// Java's <c>KafkaFuture</c> implements.</item>
/// </list>
/// </remarks>
public readonly struct KafkaFuture<T> : IEquatable<KafkaFuture<T>>
{
    private readonly SyncFutureSource<T>? _source;

    // A plain field store: it never throws, and the value holds no native resource (S-1).
    internal KafkaFuture(SyncFutureSource<T> source) => _source = source;

    /// <summary>Java <c>Future.get()</c>.</summary>
    /// <returns>
    /// The operation's result once it has completed — the same instance on every call. Blocks the calling thread until
    /// then; if the operation failed, rethrows its exception (the same instance on every call, not wrapped).
    /// </returns>
    /// <exception cref="InvalidOperationException">A default value, or (D9) for a producer send's future, a call on
    /// that producer's send-completion thread for a send that has not completed.</exception>
    public T Get() => Source.Get();

    /// <summary>
    /// Java <c>Future.isDone()</c>: whether the operation has completed, successfully or not. Never blocks.
    /// </summary>
    /// <exception cref="InvalidOperationException">A default value.</exception>
    public bool IsDone => Source.IsDone;

    // No Get(TimeSpan) (M11/P4.2 D7, kept by M15/P14 Q1; FU-4: a timed or cancellable Get).

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture{T}"/> values are equal — they are copies of the same future.
    /// </summary>
    public static bool operator ==(KafkaFuture<T> left, KafkaFuture<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture{T}"/> values are not equal — they are different futures.
    /// </summary>
    public static bool operator !=(KafkaFuture<T> left, KafkaFuture<T> right) => !left.Equals(right);

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — both are copies of the same future (reference
    /// identity of what they wait on, never the result). Two <see langword="default"/> values are equal.
    /// </summary>
    public bool Equals(KafkaFuture<T> other) => ReferenceEquals(_source, other._source);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is KafkaFuture<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _source?.GetHashCode() ?? 0;

    /// <summary>
    /// This future without its value (Java's <c>KafkaFuture&lt;Void&gt;</c>): a <see cref="KafkaFuture"/> over the
    /// <b>same</b> source — no new completion, no copy — so it completes exactly when this one does, with the same
    /// failure. A <see langword="default"/> value erases to a <see langword="default"/> one.
    /// </summary>
    internal KafkaFuture Erase() => _source is null ? default : new KafkaFuture(_source);

    /// <summary>
    /// What this future waits on, for a composite built over it — a sync admin result's projection or all-of (M15/P14
    /// S4, <see cref="SyncAdminFutures"/>). Never blocks.
    /// </summary>
    /// <exception cref="InvalidOperationException">A default value.</exception>
    internal SyncFutureSource<T> Source => _source ?? throw new InvalidOperationException(KafkaFuture.DefaultValueMessage);
}

/// <summary>
/// The handle on an operation that completes <b>without a value</b>, waited for by <b>blocking</b> — Java's
/// <c>org.apache.kafka.common.KafkaFuture&lt;Void&gt;</c>. <see cref="Get()"/> blocks until the operation completes,
/// and rethrows its failure (Java's <c>get()</c>). Only the <c>IAdmin</c> result types return a usable one;
/// <see cref="AsyncKafkaFuture"/> is the async counterpart.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><b>Get blocks the calling thread</b> on a managed latch — not a <c>Task</c>, so this is not
/// sync-over-async. Call it any number of times from any number of threads; every call returns, or rethrows the same
/// exception.</item>
/// <item><b>IsDone never blocks.</b> <see cref="IsDone"/> is <see langword="true"/> once the operation has completed,
/// successfully or not (Java's <c>isDone()</c>).</item>
/// <item><b>Failure.</b> <see cref="Get()"/> rethrows the operation's exception itself (a <see cref="KafkaException"/>
/// for a Kafka failure), not wrapped in Java's <c>ExecutionException</c>.</item>
/// <item><b>default.</b> A <c>default</c> value is bound to no operation: <see cref="Get()"/> and
/// <see cref="IsDone"/> throw <see cref="InvalidOperationException"/>.</item>
/// <item><b>Equality.</b> Two values are equal exactly when they are copies of the same future — the identity of
/// what they wait on.</item>
/// <item><b>No conversion.</b> There is no public conversion from <see cref="KafkaFuture{T}"/>, and none to or from
/// <see cref="AsyncKafkaFuture"/>.</item>
/// </list>
/// </remarks>
public readonly struct KafkaFuture : IEquatable<KafkaFuture>
{
    // The default-value message, shared with KafkaFuture<T> so the two cannot drift apart (M15/P14 S1).
    internal const string DefaultValueMessage =
        "This KafkaFuture is a default value and is bound to no operation; only IProducer.Send and the IAdmin result "
        + "types return a usable one.";

    private readonly SyncFutureSource? _source;

    // A plain field store: it never throws, and the value holds no native resource.
    internal KafkaFuture(SyncFutureSource source) => _source = source;

    /// <summary>Java <c>Future.get()</c>, for an operation without a value.</summary>
    /// <remarks>
    /// Returns once the operation has completed successfully, blocking the calling thread until then; if the operation
    /// failed, rethrows its exception (the same instance on every call, not wrapped).
    /// </remarks>
    /// <exception cref="InvalidOperationException">A default value.</exception>
    public void Get() => Source.Wait();

    /// <summary>
    /// Java <c>Future.isDone()</c>: whether the operation has completed, successfully or not. Never blocks.
    /// </summary>
    /// <exception cref="InvalidOperationException">A default value.</exception>
    public bool IsDone => Source.IsDone;

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture"/> values are equal — they are copies of the same future.
    /// </summary>
    public static bool operator ==(KafkaFuture left, KafkaFuture right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture"/> values are not equal — they are different futures.
    /// </summary>
    public static bool operator !=(KafkaFuture left, KafkaFuture right) => !left.Equals(right);

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — both are copies of the same future (reference
    /// identity of what they wait on). Two <see langword="default"/> values are equal.
    /// </summary>
    public bool Equals(KafkaFuture other) => ReferenceEquals(_source, other._source);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is KafkaFuture other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _source?.GetHashCode() ?? 0;

    /// <inheritdoc cref="KafkaFuture{T}.Source"/>
    internal SyncFutureSource Source => _source ?? throw new InvalidOperationException(DefaultValueMessage);
}
