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
using System.Threading.Tasks;

namespace Confluent.Kafka;

// Both async futures live in this file, side by side, as ValueTask and ValueTask<TResult> do in the runtime: the
// non-generic one is AsyncKafkaFuture<T> without a value, and its shape and equality mirror the generic one member for
// member (M15/P14 S1).

/// <summary>
/// The handle on an operation's result that you wait for by <b>awaiting</b> — the .NET realization of the
/// <c>java.util.concurrent.Future&lt;RecordMetadata&gt;</c> that Java's <c>Producer.send</c> returns
/// (<c>Producer.java:81</c>, <c>:86</c>), and of Java's <c>org.apache.kafka.common.KafkaFuture&lt;T&gt;</c>.
/// <see cref="Get"/> returns the result's <see cref="Task{TResult}"/>; <c>await future.Get()</c> is Java's
/// <c>future.get()</c>. Only <c>IAsyncProducer.Send</c> and the <c>IAsyncAdmin</c> result types return a usable one;
/// <see cref="KafkaFuture{T}"/> is the sync counterpart.
/// </summary>
/// <typeparam name="T">
/// The operation's result: <see cref="RecordMetadata"/> for a producer send, or the value an admin result's future carries.
/// </typeparam>
/// <remarks>
/// For a producer send, the <see cref="ValueTask{TResult}"/> returned by
/// <see cref="IAsyncProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, System.Threading.CancellationToken)"/>
/// completes when the record is <b>accepted</b> and yields this value; awaiting <see cref="Get"/> yields the
/// <see cref="RecordMetadata"/> once the record is <b>delivered</b>:
/// <code>
/// AsyncKafkaFuture&lt;RecordMetadata&gt; future = await producer.Send(record);   // accepted
/// RecordMetadata metadata = await future.Get();                            // delivered
/// </code>
/// <list type="bullet">
/// <item><description>
/// <b>It does not block; await it.</b> A <c>readonly struct</c> over the result's <see cref="Task{TResult}"/>, so it
/// costs no allocation (the common, unsaturated send allocates nothing for its first stage). <see cref="Get"/>
/// returns the <b>same</b> task on every call and never blocks; that task may be awaited any number of times and
/// stored freely — unlike a <see cref="ValueTask{TResult}"/>, which is awaited once.
/// </description></item>
/// <item><description>
/// <b>Not awaitable itself</b> — await <see cref="Get"/>. <c>ConfigureAwait</c> goes on the task:
/// <c>await future.Get().ConfigureAwait(false)</c>.
/// </description></item>
/// <item><description>
/// <b>⚠ A producer send: acceptance is not release.</b> Receiving this value (acceptance) does <b>not</b> end the
/// borrow of the record's key / value buffers. Reuse a buffer only after <see cref="Get"/>'s task completes
/// <b>without</b> being canceled, after the record's delivery callback (<see cref="IDeliveryCallback"/>) fires, or
/// after a later <see cref="IAsyncProducer{TKey, TValue}.Flush(System.Threading.CancellationToken)"/> completes
/// successfully (M11/P3.5 91.11; see <c>IAsyncProducer.Send</c>'s remarks).
/// </description></item>
/// <item><description>
/// <b>A producer send: cancellation.</b> A token that fires after acceptance cancels <see cref="Get"/>'s task; it
/// never un-sends the record.
/// </description></item>
/// <item><description>
/// <b>Default value.</b> <c>default(AsyncKafkaFuture&lt;T&gt;)</c> is bound to no operation: <see cref="Get"/> throws
/// <see cref="InvalidOperationException"/>, synchronously.
/// </description></item>
/// <item><description>
/// <b>Equality.</b> Two values are equal exactly when they wrap the same task (reference identity) — the identity of
/// Java's <c>Future</c> and of <see cref="ValueTask{TResult}"/>'s equality. There is no <c>ToString</c> override
/// (Java's <c>Future</c> has none).
/// </description></item>
/// <item><description>
/// <b>Naming and shape — a deliberate deviation.</b> Java has one future type with a blocking <c>get()</c>; this
/// binding has two, named as a pair: <see cref="KafkaFuture{T}"/> blocks, and this one hands back a task to await. It
/// is a struct over that task rather than the bare <see cref="Task{TResult}"/>, so that what Java returns as a
/// <c>Future</c> object stays a distinct value — the two stages of a producer <c>Send</c> stay distinguishable (a
/// <c>ValueTask&lt;Task&lt;T&gt;&gt;</c> reads as one awaitable too many) — and it costs nothing at runtime. For the
/// producer the name says <i>KafkaFuture</i> although Java's type there is <c>java.util.concurrent.Future</c>, which
/// Java's <c>KafkaFuture</c> implements.
/// </description></item>
/// </list>
/// </remarks>
public readonly struct AsyncKafkaFuture<T> : IEquatable<AsyncKafkaFuture<T>>
{
    private readonly Task<T>? _delivery;

    // Never throws: production builds this after the record is appended (the M11/P3.5 post-append
    // no-throw invariant), so it must stay a plain field store.
    internal AsyncKafkaFuture(Task<T> delivery) => _delivery = delivery;

    /// <summary>
    /// Returns the result's task: it resolves with the value, or faults with the operation's exception (a
    /// <see cref="KafkaException"/> for a Kafka failure); a producer send's task is also canceled by the send's token.
    /// Await it for Java's <c>future.get()</c>.
    /// </summary>
    /// <returns>The <see cref="Task{TResult}"/> — the same instance on every call. This call never blocks.</returns>
    /// <exception cref="InvalidOperationException">
    /// This is a <see langword="default"/> value, which is bound to no operation. Thrown synchronously, by this call.
    /// </exception>
    public Task<T> Get() =>
        _delivery ?? throw new InvalidOperationException(AsyncKafkaFuture.DefaultValueMessage);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> values are equal — they wrap the same task.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture<T> left, AsyncKafkaFuture<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> values are not equal — they wrap different tasks.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture<T> left, AsyncKafkaFuture<T> right) => !left.Equals(right);

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — both wrap the same task (reference identity,
    /// never the task's result). Two <see langword="default"/> values are equal.
    /// </summary>
    public bool Equals(AsyncKafkaFuture<T> other) => ReferenceEquals(_delivery, other._delivery);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is AsyncKafkaFuture<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _delivery?.GetHashCode() ?? 0;

    /// <summary>
    /// This future without its value (Java's <c>KafkaFuture&lt;Void&gt;</c>): an <see cref="AsyncKafkaFuture"/> over the
    /// <b>same</b> task — no new task, no continuation — so it completes exactly when this one does, with the same
    /// failure. A <see langword="default"/> value erases to a <see langword="default"/> one.
    /// </summary>
    internal AsyncKafkaFuture Erase() => _delivery is null ? default : new AsyncKafkaFuture(_delivery);
}

/// <summary>
/// The handle on an operation that completes <b>without a value</b>, waited for by <b>awaiting</b> — Java's
/// <c>org.apache.kafka.common.KafkaFuture&lt;Void&gt;</c>. <see cref="Get"/> returns the operation's
/// <see cref="Task"/>; <c>await future.Get()</c> is Java's <c>future.get()</c>. Only the <c>IAsyncAdmin</c> result
/// types return a usable one; <see cref="KafkaFuture"/> is the sync counterpart.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>It does not block; await it.</b> A <c>readonly struct</c> over the operation's <see cref="Task"/>.
/// <see cref="Get"/> returns the <b>same</b> task on every call and never blocks; that task may be awaited any number
/// of times and stored freely.
/// </description></item>
/// <item><description>
/// <b>Not awaitable itself</b> — await <see cref="Get"/>. <c>ConfigureAwait</c> goes on the task.
/// </description></item>
/// <item><description>
/// <b>Default value.</b> <c>default(AsyncKafkaFuture)</c> is bound to no operation: <see cref="Get"/> throws
/// <see cref="InvalidOperationException"/>, synchronously.
/// </description></item>
/// <item><description>
/// <b>Equality.</b> Two values are equal exactly when they wrap the same task (reference identity). There is no
/// <c>ToString</c> override.
/// </description></item>
/// <item><description>
/// <b>No conversion.</b> There is no public conversion from <see cref="AsyncKafkaFuture{T}"/>, and none to or from
/// <see cref="KafkaFuture"/>.
/// </description></item>
/// </list>
/// </remarks>
public readonly struct AsyncKafkaFuture : IEquatable<AsyncKafkaFuture>
{
    // The default-value message, shared with AsyncKafkaFuture<T> so the two cannot drift apart (M15/P14 S1).
    internal const string DefaultValueMessage =
        "This AsyncKafkaFuture is a default value and is bound to no operation; only IAsyncProducer.Send and the "
        + "IAsyncAdmin result types return a usable one.";

    private readonly Task? _task;

    // A plain field store: it never throws.
    internal AsyncKafkaFuture(Task task) => _task = task;

    /// <summary>
    /// Returns the operation's task: it completes when the operation succeeds, or faults with its exception (a
    /// <see cref="KafkaException"/> for a Kafka failure). Await it for Java's <c>future.get()</c>.
    /// </summary>
    /// <returns>The <see cref="Task"/> — the same instance on every call. This call never blocks.</returns>
    /// <exception cref="InvalidOperationException">
    /// This is a <see langword="default"/> value, which is bound to no operation. Thrown synchronously, by this call.
    /// </exception>
    public Task Get() => _task ?? throw new InvalidOperationException(DefaultValueMessage);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture"/> values are equal — they wrap the same task.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture left, AsyncKafkaFuture right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture"/> values are not equal — they wrap different tasks.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture left, AsyncKafkaFuture right) => !left.Equals(right);

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — both wrap the same task (reference identity).
    /// Two <see langword="default"/> values are equal.
    /// </summary>
    public bool Equals(AsyncKafkaFuture other) => ReferenceEquals(_task, other._task);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is AsyncKafkaFuture other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _task?.GetHashCode() ?? 0;
}
